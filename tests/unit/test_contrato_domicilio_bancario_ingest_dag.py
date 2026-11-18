"""Comportamento da ingestão do domicílio bancário por contrato (issue #33).

A API respondeu 404 "sem domicílio" em todos os contratos consultados, então
nenhum payload aqui foi observado na fonte. DOMICILIO_SINTETICO segue a
estrutura do schema `DomicilioBancario` da spec OpenAPI
(https://contratos.comprasnet.gov.br/docs/api-docs.json) e de
ContratoController::formataOperacoesDomicilioBancario no código-fonte; os
valores bancários são fictícios, com zeros à esquerda de propósito.

Os testes provam que a DAG grava o objeto como veio, uma linha por contrato,
com contrato_id como chave e domiciliosbancarios como JSON. Eles NÃO provam
que a fonte entrega esse formato em dado real: isso continua pendente de
homologação, com o primeiro domicílio real (catalogo/sistemas/contratos_gov.yml).
"""

import copy
import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import fsspec
import polars as pl
import pytest
from airflow.models import DagBag

import landing_zone
from cliente_contratos_gov import ContratoNaoLocalizado
from landing_zone import RawIndisponivel

pytestmark = pytest.mark.unit

DAG_FILE = (
    Path(__file__).resolve().parents[2]
    / "airflow"
    / "dags"
    / "data_ingest"
    / "contratos_gov"
    / "contrato_domicilio_bancario_ingest_dag.py"
)
DAG_ID = "contrato_domicilio_bancario_ingest_dag"

OPERACAO_REGISTRADA = {
    "id": 1,
    "situacao": "Registrado",
    "banco": "000",
    "agencia": "0001",
    "conta_bancaria": "0000012345",
    "codigo_operacao_credito": "0001",
    "data_registro": "31/10/2023 14:11:23",
    "updated_at": "29/11/2024 10:06:04",
}
OPERACAO_LIQUIDADA = {
    **OPERACAO_REGISTRADA,
    "id": 2,
    "situacao": "Liquidado",
    "codigo_operacao_credito": "0002",
    "data_registro": "15/01/2024 09:30:00",
    "updated_at": "15/01/2024 09:30:00",
}
DOMICILIO_SINTETICO = {
    "unidade_gestora": "012345",
    "contrato_id": 115,
    "contrato": "00030/2023",
    "domiciliosbancarios": [OPERACAO_REGISTRADA, OPERACAO_LIQUIDADA],
}
CAMPOS_OPENAPI = set(DOMICILIO_SINTETICO)


def domicilio(contrato_id: int = 115, **campos: object) -> dict:
    """Cópia independente do domicílio sintético, com campos sobrescritos."""
    return {**copy.deepcopy(DOMICILIO_SINTETICO), "contrato_id": contrato_id, **campos}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FILE), include_examples=False)


def task(dagbag: DagBag, task_id: str):
    return dagbag.dags[DAG_ID].get_task(task_id).python_callable


def preparar_ingestao(ingerir, monkeypatch: pytest.MonkeyPatch, respostas):
    """Troca o cliente e o write_raw da task; devolve os dois mocks.

    `respostas` é a sequência do que consultar_domicilio_bancario devolve (ou
    levanta, se for exceção), um item por contrato, ou uma função.
    """
    cliente = MagicMock()
    if callable(respostas):
        cliente.consultar_domicilio_bancario.side_effect = respostas
    else:
        cliente.consultar_domicilio_bancario.side_effect = list(respostas)
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)
    return cliente, escrita


def resultado(**campos: object) -> dict:
    """Retorno de ingest_contracts com zeros nos campos não informados."""
    return {
        "contratos_com_domicilio": 0,
        "operacoes": 0,
        "contratos_sem_domicilio": 0,
        "contratos_nao_localizados": [],
        "operacoes_repetidas": 0,
        **campos,
    }


# --- definição da DAG --------------------------------------------------------


def test_dag_carrega_no_horario_do_escalonamento(dagbag: DagBag) -> None:
    """18:30 de sábado: a janela dos sub-recursos começa às 12:00 e escalona de
    30 em 30 minutos na ordem das fases da issue #15; a #33 é a 14ª, depois
    das ocorrências (#32) às 18:00 (docs/notas/contratos-gov-ingestao.md)."""
    assert dagbag.import_errors == {}, dagbag.import_errors
    assert dagbag.dags[DAG_ID].timetable.expression == "30 18 * * 6"


def test_metadados_seguem_adr_0007_e_0008(dagbag: DagBag) -> None:
    dag = dagbag.dags[DAG_ID]
    assert dag.dag_id == DAG_FILE.stem
    assert set(dag.tags) == {"sistema:contratos_gov", "dominio:contratacoes"}
    assert dag.default_args["owner"] == "mgi"
    assert dag.default_args["queue"] == "mgi"
    assert dag.default_args["retries"] == 3
    assert "contratos_gov.raw_contrato_domicilio_bancario" in dag.description


def test_ingestao_limita_a_concorrencia_contra_a_api(dagbag: DagBag) -> None:
    """Mesmo valor do teste de carga e das DAGs vizinhas."""
    assert dagbag.dags[DAG_ID].get_task("ingest_contracts").max_active_tis_per_dag == 4


def test_expande_por_bloco_e_valida_no_fim(dagbag: DagBag) -> None:
    """O .expand() é sobre a saída de get_contract_blocks (blocos), nunca
    sobre a lista de contratos; validate só roda depois de todos os blocos."""
    dag = dagbag.dags[DAG_ID]
    assert set(dag.task_ids) == {"get_contract_blocks", "ingest_contracts", "validate"}
    ingerir = dag.get_task("ingest_contracts")
    assert ingerir.upstream_task_ids == {"get_contract_blocks"}
    # Task decorada guarda o mapeamento em op_kwargs_expand_input.
    mapeado = ingerir.op_kwargs_expand_input.value
    assert set(mapeado) == {"contrato_ids"}
    assert mapeado["contrato_ids"].operator.task_id == "get_contract_blocks"
    assert dag.get_task("validate").upstream_task_ids == {"ingest_contracts"}
    assert dag.get_task("validate").downstream_task_ids == set()


def test_dag_nao_importa_cliente_de_banco() -> None:
    """ADR-0011 e ADR-0021: a DAG só conhece write_raw."""
    fonte = DAG_FILE.read_text()
    assert "cliente_postgres" not in fonte
    assert "psycopg2" not in fonte
    assert "from landing_zone import write_raw" in fonte


# --- get_contract_blocks: escopo, limite local e particionamento -------------


def test_le_contratos_ativos_e_inativos_pelo_escopo_de_orgaos(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercita o helper real de escopo: une ativos e inativos, filtra pelo
    órgão e remove o id que aparece nas duas raws."""
    preparar = task(dagbag, "get_contract_blocks")
    escopo = preparar.__globals__["ids_contratos_no_escopo"]
    linhas = {
        "contrato_ativo": [(115, "46000"), (2289, "46000"), (999, "17000")],
        "contrato_inativo": [(115, "46000"), (47, "46000")],
    }
    lidas: list[tuple[str, str, list[str]]] = []

    def raw_falsa(sistema: str, entidade: str, colunas: list[str]):
        lidas.append((sistema, entidade, colunas))
        return linhas[entidade]

    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", lambda: {"46000"})
    monkeypatch.setitem(escopo.__globals__, "distinct_raw_rows", raw_falsa)
    monkeypatch.delenv("INGEST_MAX_CONTRATOS", raising=False)

    assert preparar() == [["47", "115", "2289"]]
    assert lidas == [
        ("contratos_gov", "contrato_ativo", ["id", "orgao_codigo"]),
        ("contratos_gov", "contrato_inativo", ["id", "orgao_codigo"]),
    ]


@pytest.mark.parametrize("entidade_ausente", ["contrato_ativo", "contrato_inativo"])
def test_raw_de_cabecalho_ausente_diz_qual_dag_rodar_antes(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, entidade_ausente: str
) -> None:
    preparar = task(dagbag, "get_contract_blocks")
    escopo = preparar.__globals__["ids_contratos_no_escopo"]

    def raw_falsa(sistema: str, entidade: str, colunas: list[str]):
        if entidade == entidade_ausente:
            raise RawIndisponivel(f"{sistema}.{entidade} não existe")
        return [(115, "46000")]

    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", lambda: {"46000"})
    monkeypatch.setitem(escopo.__globals__, "distinct_raw_rows", raw_falsa)

    with pytest.raises(RuntimeError, match=f"Execute {entidade_ausente}_ingest_dag"):
        preparar()


def test_nenhum_contrato_no_escopo_falha_antes_de_expandir(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Raws presentes, mas sem contrato dos órgãos do escopo: não há o que
    consultar, e o caso é anomalia de configuração, não execução vazia."""
    preparar = task(dagbag, "get_contract_blocks")
    escopo = preparar.__globals__["ids_contratos_no_escopo"]
    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", lambda: {"46000"})
    monkeypatch.setitem(
        escopo.__globals__, "distinct_raw_rows", lambda *_: [(999, "17000")]
    )

    with pytest.raises(RuntimeError, match="Nenhum contrato dos órgãos"):
        preparar()


def test_limita_antes_de_particionar(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    preparar = task(dagbag, "get_contract_blocks")
    escopo = MagicMock(return_value={"46000"})
    ids = MagicMock(return_value=[str(i) for i in range(61)])
    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", escopo)
    monkeypatch.setitem(preparar.__globals__, "ids_contratos_no_escopo", ids)
    monkeypatch.setenv("INGEST_MAX_CONTRATOS", "51")

    blocos = preparar()

    # Blocos normais de 25; o teto corta a lista antes, sem reordenar.
    assert [len(bloco) for bloco in blocos] == [25, 25, 1]
    assert [id_ for bloco in blocos for id_ in bloco] == [str(i) for i in range(51)]
    ids.assert_called_once_with({"46000"})


@pytest.mark.parametrize("valor", ["", "abc", "-1"], ids=["vazio", "texto", "negativo"])
def test_limite_local_invalido_nao_trunca(
    dagbag: DagBag,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    valor: str,
) -> None:
    """batching.limit_local ignora valor vazio, não inteiro ou negativo: a
    lista segue inteira, e só o texto não numérico gera aviso."""
    preparar = task(dagbag, "get_contract_blocks")
    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", lambda: {"46000"})
    monkeypatch.setitem(
        preparar.__globals__,
        "ids_contratos_no_escopo",
        lambda _: [str(i) for i in range(30)],
    )
    monkeypatch.setenv("INGEST_MAX_CONTRATOS", valor)

    with caplog.at_level(logging.WARNING):
        blocos = preparar()

    assert [len(bloco) for bloco in blocos] == [25, 5]
    assert ("não é inteiro" in caplog.text) == (valor == "abc")


def test_limite_local_zerado_falha_antes_de_expandir(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    preparar = task(dagbag, "get_contract_blocks")
    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", lambda: {"46000"})
    monkeypatch.setitem(
        preparar.__globals__, "ids_contratos_no_escopo", lambda _: ["115", "2289"]
    )
    monkeypatch.setenv("INGEST_MAX_CONTRATOS", "0")

    with pytest.raises(RuntimeError, match="INGEST_MAX_CONTRATOS"):
        preparar()


@pytest.mark.parametrize(
    ("quantidade", "maior_bloco"),
    [(25 * 1024, 25), (25 * 1024 + 1, 26), (26000, 26)],
    ids=["exatamente_1024_blocos", "um_alem_do_limite", "volume_alto"],
)
def test_bloco_cresce_para_nao_exceder_o_limite_de_mapeamento(
    dagbag: DagBag,
    monkeypatch: pytest.MonkeyPatch,
    quantidade: int,
    maior_bloco: int,
) -> None:
    """core.max_map_length é 1024: o bloco só passa de 25 quando 25 por bloco
    geraria mais de 1024 mapped task instances."""
    preparar = task(dagbag, "get_contract_blocks")
    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", lambda: {"46000"})
    monkeypatch.setitem(
        preparar.__globals__,
        "ids_contratos_no_escopo",
        lambda _: [str(i) for i in range(quantidade)],
    )
    monkeypatch.delenv("INGEST_MAX_CONTRATOS", raising=False)

    blocos = preparar()

    assert len(blocos) <= 1024
    assert max(map(len, blocos)) == maior_bloco
    assert [id_ for bloco in blocos for id_ in bloco] == [
        str(i) for i in range(quantidade)
    ]


# --- ingest_contracts: chamada, escrita e validação do objeto ----------------


def test_domicilio_e_gravado_como_um_objeto_sem_alteracao(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Uma linha por contrato: o objeto da fonte vai inteiro, nenhum campo
    renomeado, achatado ou descartado, e domiciliosbancarios continua lista de
    objetos até o write_raw."""
    ingerir = task(dagbag, "ingest_contracts")
    original = domicilio()
    cliente, escrita = preparar_ingestao(ingerir, monkeypatch, [domicilio(), None])

    assert ingerir(["115", "2289"]) == resultado(
        contratos_com_domicilio=1, operacoes=2, contratos_sem_domicilio=1
    )
    assert cliente.consultar_domicilio_bancario.call_args_list == [
        call("115"),
        call("2289"),
    ]
    escrita.assert_called_once_with(
        "contratos_gov",
        "contrato_domicilio_bancario",
        [original],
        primary_key=["contrato_id"],
        run_id=None,
        json_fields=["domiciliosbancarios"],
    )
    gravado = escrita.call_args.args[2][0]
    assert set(gravado) == CAMPOS_OPENAPI
    assert gravado["domiciliosbancarios"] == [OPERACAO_REGISTRADA, OPERACAO_LIQUIDADA]


def test_campos_bancarios_continuam_texto_com_zero_a_esquerda(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    ingerir = task(dagbag, "ingest_contracts")
    _, escrita = preparar_ingestao(ingerir, monkeypatch, [domicilio()])

    ingerir(["115"])

    gravado = escrita.call_args.args[2][0]
    assert gravado["unidade_gestora"] == "012345"
    operacao = gravado["domiciliosbancarios"][0]
    assert operacao["banco"] == "000"
    assert operacao["agencia"] == "0001"
    assert operacao["conta_bancaria"] == "0000012345"
    assert operacao["codigo_operacao_credito"] == "0001"


def test_cada_contrato_grava_com_run_id_proprio(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    ingerir = task(dagbag, "ingest_contracts")
    _, escrita = preparar_ingestao(
        ingerir, monkeypatch, [domicilio(115), domicilio(2289)]
    )
    monkeypatch.setitem(
        ingerir.__globals__,
        "get_current_context",
        lambda: {"run_id": "manual__2026-11-17"},
    )

    ingerir(["115", "2289"])

    assert [chamada.kwargs["run_id"] for chamada in escrita.call_args_list] == [
        "manual__2026-11-17__contrato_115",
        "manual__2026-11-17__contrato_2289",
    ]


def test_contratos_sem_domicilio_nao_geram_escrita(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """404 "sem domicílio" (None no cliente) é a resposta comum: conta como
    vazio e não derruba o bloco."""
    ingerir = task(dagbag, "ingest_contracts")
    _, escrita = preparar_ingestao(ingerir, monkeypatch, [None, None])

    assert ingerir(["115", "2289"]) == resultado(contratos_sem_domicilio=2)
    escrita.assert_not_called()


def test_contrato_nao_localizado_e_contado_a_parte_sem_derrubar_o_bloco(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Contrato apagado na fonte depois da varredura de cabeçalhos continua na
    raw append-only do object_storage; derrubar o bloco por ele travaria a DAG
    para sempre. Ele não vira "sem domicílio": é contado e avisado."""
    ingerir = task(dagbag, "ingest_contracts")
    _, escrita = preparar_ingestao(
        ingerir,
        monkeypatch,
        [ContratoNaoLocalizado("não localizado"), domicilio(2289), None],
    )

    with caplog.at_level(logging.WARNING):
        retorno = ingerir(["115", "2289", "47"])

    assert retorno == resultado(
        contratos_com_domicilio=1,
        operacoes=2,
        contratos_sem_domicilio=1,
        contratos_nao_localizados=["115"],
    )
    assert escrita.call_count == 1
    assert "Contrato 115: não localizado na fonte" in caplog.text


@pytest.mark.parametrize(
    "erro",
    [RuntimeError("status 500"), RuntimeError("status 503"), ValueError("x")],
    ids=["http_500", "timeout_esgotado", "outro"],
)
def test_erro_da_api_interrompe_o_bloco(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, erro: Exception
) -> None:
    """Falha real da API não vira contrato sem domicílio: a task falha e o
    Airflow refaz o bloco (retries em default_args)."""
    ingerir = task(dagbag, "ingest_contracts")
    _, escrita = preparar_ingestao(ingerir, monkeypatch, [None, erro])

    with pytest.raises(type(erro)):
        ingerir(["115", "2289"])

    escrita.assert_not_called()


def test_operacao_identica_repetida_e_mantida_e_contada(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A raw guarda o objeto da fonte, e o vínculo contrato_antecipagov aceita o
    mesmo par duas vezes: a cópia exata fica para a Silver deduplicar, mas não
    passa em silêncio."""
    ingerir = task(dagbag, "ingest_contracts")
    repetido = domicilio(
        domiciliosbancarios=[OPERACAO_REGISTRADA, copy.deepcopy(OPERACAO_REGISTRADA)]
    )
    _, escrita = preparar_ingestao(ingerir, monkeypatch, [repetido])

    with caplog.at_level(logging.WARNING):
        retorno = ingerir(["115"])

    assert retorno == resultado(
        contratos_com_domicilio=1, operacoes=2, operacoes_repetidas=1
    )
    assert escrita.call_args.args[2][0]["domiciliosbancarios"] == [
        OPERACAO_REGISTRADA,
        OPERACAO_REGISTRADA,
    ]
    assert "mantida(s) como a fonte entregou" in caplog.text


def test_repeticao_e_contada_pelo_id_e_nao_pela_identidade_do_objeto(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O mesmo objeto duas vezes na lista também é repetição: o critério é o
    id já visto, não ser outra instância de dict."""
    ingerir = task(dagbag, "ingest_contracts")
    operacao = copy.deepcopy(OPERACAO_REGISTRADA)
    preparar_ingestao(
        ingerir, monkeypatch, [domicilio(domiciliosbancarios=[operacao, operacao])]
    )

    assert ingerir(["115"])["operacoes_repetidas"] == 1


def test_lista_vazia_com_200_e_gravada_com_aviso(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A fonte responde 404 quando não há operação; se um dia responder 200 com
    lista vazia, a raw guarda o que veio e o log registra a divergência."""
    ingerir = task(dagbag, "ingest_contracts")
    _, escrita = preparar_ingestao(
        ingerir, monkeypatch, [domicilio(domiciliosbancarios=[])]
    )

    with caplog.at_level(logging.WARNING):
        retorno = ingerir(["115"])

    assert retorno == resultado(contratos_com_domicilio=1)
    assert escrita.call_args.args[2][0]["domiciliosbancarios"] == []
    assert "domiciliosbancarios vazio" in caplog.text


def _sem(campo: str) -> dict:
    return {k: v for k, v in domicilio().items() if k != campo}


@pytest.mark.parametrize(
    ("payload", "mensagem"),
    [
        (domicilio(99999), "contrato_id divergente da URL"),
        (_sem("contrato_id"), "contrato_id divergente da URL"),
        (_sem("domiciliosbancarios"), "domiciliosbancarios ausente"),
        (domicilio(domiciliosbancarios={"id": 1}), "domiciliosbancarios ausente"),
        (
            domicilio(
                domiciliosbancarios=[
                    {k: v for k, v in OPERACAO_REGISTRADA.items() if k != "id"}
                ]
            ),
            "operação de domicílio bancário sem id",
        ),
        (domicilio(domiciliosbancarios=["001"]), "operação de domicílio bancário sem id"),
        (
            domicilio(
                domiciliosbancarios=[
                    OPERACAO_REGISTRADA,
                    {**OPERACAO_REGISTRADA, "situacao": "Cancelado"},
                ]
            ),
            "repetido com conteúdo diferente",
        ),
    ],
    ids=[
        "contrato_id_divergente",
        "sem_contrato_id",
        "sem_lista",
        "lista_como_objeto",
        "operacao_sem_id",
        "operacao_nao_objeto",
        "id_conflitante",
    ],
)
def test_objeto_invalido_falha_antes_da_escrita(
    dagbag: DagBag,
    monkeypatch: pytest.MonkeyPatch,
    payload: dict,
    mensagem: str,
) -> None:
    """O cliente é mockado, então estes casos exercitam só a segunda barreira
    (_validar_domicilio); a primeira, no cliente, é coberta em
    test_cliente_contratos_gov.py."""
    ingerir = task(dagbag, "ingest_contracts")
    _, escrita = preparar_ingestao(ingerir, monkeypatch, [payload])

    with pytest.raises(RuntimeError, match=mensagem):
        ingerir(["115"])

    escrita.assert_not_called()


def test_contrato_id_igual_em_tipo_diferente_nao_e_divergencia(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fonte devolve int; a URL é montada com o id da raw, em texto. A raw
    fica com o valor da origem."""
    ingerir = task(dagbag, "ingest_contracts")
    _, escrita = preparar_ingestao(ingerir, monkeypatch, [domicilio(115)])

    ingerir(["115"])

    assert escrita.call_args.args[2][0]["contrato_id"] == 115


# --- backends da zona raw (ADR-0021) -----------------------------------------


def test_object_storage_mantem_arquivos_por_contrato_e_execucao(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Append-only: retry e nova execução criam arquivos novos, sem sobrescrever
    os anteriores; a Silver deduplica por contrato_id e dt_ingest."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.consultar_domicilio_bancario.side_effect = lambda contrato_id: domicilio(
        int(contrato_id)
    )
    run_id = ["primeiro"]
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", landing_zone.write_raw)
    monkeypatch.setitem(
        ingerir.__globals__, "get_current_context", lambda: {"run_id": run_id[0]}
    )
    monkeypatch.setattr(
        landing_zone, "get_storage_fs", lambda: fsspec.filesystem("file", auto_mkdir=True)
    )
    monkeypatch.setattr(landing_zone, "get_bucket", lambda: str(tmp_path))
    monkeypatch.setenv("RAW_BACKEND", "object_storage")

    assert ingerir(["115", "2289"])["contratos_com_domicilio"] == 2
    # Retry da mesma DagRun também deve preservar os arquivos anteriores.
    assert ingerir(["115", "2289"])["contratos_com_domicilio"] == 2
    run_id[0] = "segundo"
    assert ingerir(["115", "2289"])["contratos_com_domicilio"] == 2

    arquivos = sorted(tmp_path.rglob("*.parquet"))
    assert len(arquivos) == 6
    assert all("contratos_gov/contrato_domicilio_bancario/" in str(a) for a in arquivos)
    identificadores = [arquivo.stem.split("--lote-")[0] for arquivo in arquivos]
    assert sorted(identificadores) == sorted(
        ["primeiro__contrato_115", "primeiro__contrato_2289"] * 2
        + ["segundo__contrato_115", "segundo__contrato_2289"]
    )
    for arquivo in arquivos:
        tabela = pl.read_parquet(arquivo)
        assert tabela.height == 1
        assert tabela.schema["unidade_gestora"] == pl.String
        registro = tabela.to_dicts()[0]
        assert str(registro["contrato_id"]) in arquivo.stem
        assert registro["dt_ingest"]
        assert registro["unidade_gestora"] == "012345"
        # O Parquet guarda domiciliosbancarios como lista de struct, com os
        # campos bancários em texto: os zeros à esquerda sobrevivem.
        assert registro["domiciliosbancarios"] == [
            OPERACAO_REGISTRADA,
            OPERACAO_LIQUIDADA,
        ]


def test_warehouse_grava_lista_como_json_com_chave_contrato_id(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No backend warehouse, a mesma chamada vira upsert por contrato_id em
    contratos_gov.raw_contrato_domicilio_bancario, com domiciliosbancarios em
    JSON recuperável (sem json_fields, o cliente de banco gravaria o repr
    Python da lista) e o payload em memória intacto."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.consultar_domicilio_bancario.return_value = domicilio()
    banco = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", landing_zone.write_raw)
    monkeypatch.setenv("RAW_BACKEND", "warehouse")

    with patch.dict(
        "sys.modules",
        {
            "cliente_postgres": MagicMock(ClientPostgresDB=lambda _: banco),
            "postgres_helpers": MagicMock(get_postgres_conn=lambda _: "conn"),
        },
    ):
        assert ingerir(["115"])["contratos_com_domicilio"] == 1

    registros, tabela = banco.insert_data.call_args.args
    kwargs = banco.insert_data.call_args.kwargs
    assert tabela == "raw_contrato_domicilio_bancario"
    assert kwargs["schema"] == "contratos_gov"
    assert kwargs["primary_key"] == kwargs["conflict_fields"] == ["contrato_id"]
    assert len(registros) == 1
    assert set(registros[0]) == CAMPOS_OPENAPI | {"dt_ingest"}
    assert registros[0]["dt_ingest"]
    assert registros[0]["unidade_gestora"] == "012345"
    serializado = registros[0]["domiciliosbancarios"]
    assert isinstance(serializado, str)
    assert '"agencia": "0001"' in serializado
    assert json.loads(serializado) == [OPERACAO_REGISTRADA, OPERACAO_LIQUIDADA]


# --- validate ----------------------------------------------------------------


def test_validacao_soma_os_blocos(
    dagbag: DagBag, caplog: pytest.LogCaptureFixture
) -> None:
    validar = task(dagbag, "validate")

    with caplog.at_level(logging.INFO):
        total = validar(
            [
                resultado(
                    contratos_com_domicilio=2,
                    operacoes=5,
                    contratos_sem_domicilio=20,
                    operacoes_repetidas=1,
                ),
                resultado(
                    contratos_com_domicilio=1, operacoes=1, contratos_sem_domicilio=24
                ),
            ]
        )

    assert total == 3
    assert (
        "3 contrato(s) gravado(s) com 6 operação(ões) em 2 blocos; sem domicílio: "
        "44; não localizados: 0; operações repetidas mantidas: 1" in caplog.text
    )
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


def test_execucao_sem_nenhum_domicilio_avisa_e_nao_fabrica_dado(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Fase 4: zero é o resultado esperado enquanto a fonte só responder 404. A
    execução termina com sucesso e avisa que a raw pode não existir — sem
    gravar linha vazia nem criar tabela para "fazer a Bronze existir"."""
    validar = task(dagbag, "validate")
    escrita = MagicMock()
    monkeypatch.setitem(validar.__globals__, "write_raw", escrita)

    with caplog.at_level(logging.WARNING):
        total = validar(
            [
                resultado(contratos_sem_domicilio=25),
                resultado(contratos_sem_domicilio=25),
            ]
        )

    assert total == 0
    escrita.assert_not_called()
    avisos = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(avisos) == 1
    mensagem = avisos[0].getMessage()
    assert "Nenhum dos 50 contrato(s) consultados tem domicílio bancário" in mensagem
    assert "contratos_gov.raw_contrato_domicilio_bancario ainda não existe" in mensagem


def test_contratos_nao_localizados_sao_avisados_com_os_ids(
    dagbag: DagBag, caplog: pytest.LogCaptureFixture
) -> None:
    validar = task(dagbag, "validate")

    with caplog.at_level(logging.WARNING):
        total = validar(
            [
                resultado(contratos_com_domicilio=1, contratos_nao_localizados=["115"]),
                resultado(contratos_sem_domicilio=3, contratos_nao_localizados=["47"]),
            ]
        )

    assert total == 1
    assert "2 contrato(s) da raw não localizado(s) na fonte" in caplog.text
    assert "115, 47" in caplog.text


def test_aviso_de_nao_localizados_e_truncado(
    dagbag: DagBag, caplog: pytest.LogCaptureFixture
) -> None:
    validar = task(dagbag, "validate")
    ids = [str(i) for i in range(30)]

    with caplog.at_level(logging.WARNING):
        validar([resultado(contratos_sem_domicilio=5, contratos_nao_localizados=ids)])

    aviso = next(
        r.getMessage() for r in caplog.records if "não localizado" in r.getMessage()
    )
    assert aviso.endswith(" ...")
    assert "19" in aviso and ", 20," not in aviso


def test_nenhum_contrato_localizado_falha(dagbag: DagBag) -> None:
    """Todos os contratos "não localizados" na mesma execução indica raw e API
    de ambientes diferentes, não contratos apagados um a um."""
    validar = task(dagbag, "validate")

    with pytest.raises(RuntimeError, match="Nenhum dos 3 contrato"):
        validar([resultado(contratos_nao_localizados=["1", "2", "3"])])
