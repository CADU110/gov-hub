"""Comportamento da ingestão das ocorrências por contrato (issue #32).

A API devolveu 200 [] em todos os contratos da amostra, então nenhum payload
aqui foi observado na fonte. OCORRENCIA_OPENAPI é sintético: usa os campos e os
valores de exemplo do schema `Ocorrencias` da spec OpenAPI
(https://contratos.comprasnet.gov.br/docs/api-docs.json), e não dado real.

Os testes provam que a DAG trata `id` como a chave declarada — vai como
primary_key e é o critério de repetição dentro do lote. Eles NÃO provam que
`id` é único na fonte: isso vem do código-fonte (comentário da entidade em
catalogo/sistemas/contratos_gov.yml) e continua pendente de confirmação em
homologação, com a primeira ocorrência real.
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
from landing_zone import RawIndisponivel

pytestmark = pytest.mark.unit

DAG_FILE = (
    Path(__file__).resolve().parents[2]
    / "airflow"
    / "dags"
    / "data_ingest"
    / "contratos_gov"
    / "contrato_ocorrencia_ingest_dag.py"
)
DAG_ID = "contrato_ocorrencia_ingest_dag"

# Exemplo de `arquivos` da spec: lista de objetos {"arquivo_N": url}, com uma
# chave diferente em cada elemento.
ARQUIVOS_OPENAPI = [
    {"arquivo_1": "localhost:8000/storage/contrato/1_00420_2019/580e4da7.pdf"},
    {"arquivo_2": "localhost:8000/storage/contrato/1_00420_2019/4e35d0c0.pdf"},
]

# Sintético, a partir dos exemplos do schema `Ocorrencias` da OpenAPI. `usuario`
# e `emailpreposto` são os próprios exemplos fictícios da spec, não pessoas.
OCORRENCIA_OPENAPI = {
    "id": 20,
    "contrato_id": 115,
    "numero": 10,
    "usuario": "***.111.111-** FULANO DE TAL",
    "data": "2020-01-01",
    "ocorrencia": "EXTRATO DE CONTRATO",
    "notificapreposto": "Sim",
    "emailpreposto": "email@cconta.com",
    "numeroocorrencia": 3,
    "novasituacao": "Atendida",
    "situacao": "Atendida Parcial",
    "arquivos": ARQUIVOS_OPENAPI,
}
CAMPOS_OPENAPI = set(OCORRENCIA_OPENAPI)


def ocorrencia(contrato_id: int = 115, **campos: object) -> dict:
    """Cópia independente da ocorrência da OpenAPI, com campos sobrescritos."""
    return {**copy.deepcopy(OCORRENCIA_OPENAPI), "contrato_id": contrato_id, **campos}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FILE), include_examples=False)


def task(dagbag: DagBag, task_id: str):
    return dagbag.dags[DAG_ID].get_task(task_id).python_callable


def preparar_ingestao(ingerir, monkeypatch: pytest.MonkeyPatch, respostas):
    """Troca o cliente e o write_raw da task; devolve os dois mocks."""
    cliente = MagicMock()
    if callable(respostas):
        cliente.listar_subrecurso.side_effect = respostas
    else:
        cliente.listar_subrecurso.side_effect = list(respostas)
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)
    return cliente, escrita


# --- definição da DAG --------------------------------------------------------


def test_dag_carrega_no_horario_do_escalonamento(dagbag: DagBag) -> None:
    """18:00 de sábado: a janela dos sub-recursos começa às 12:00 e escalona de
    30 em 30 minutos na ordem das fases da issue #15; a #32 é a 13ª
    (docs/notas/contratos-gov-ingestao.md)."""
    assert dagbag.import_errors == {}, dagbag.import_errors
    assert dagbag.dags[DAG_ID].timetable.expression == "0 18 * * 6"


def test_metadados_seguem_adr_0007_e_0008(dagbag: DagBag) -> None:
    dag = dagbag.dags[DAG_ID]
    assert dag.dag_id == DAG_FILE.stem
    assert set(dag.tags) == {"sistema:contratos_gov", "dominio:contratacoes"}
    assert dag.default_args["owner"] == "mgi"
    assert dag.default_args["queue"] == "mgi"
    assert "contratos_gov.raw_contrato_ocorrencia" in dag.description


def test_ingestao_limita_a_concorrencia_contra_a_api(dagbag: DagBag) -> None:
    assert dagbag.dags[DAG_ID].get_task("ingest_contracts").max_active_tis_per_dag == 4


def test_expande_por_bloco_e_valida_no_fim(dagbag: DagBag) -> None:
    """O .expand() é sobre a saída de get_contract_blocks (blocos), nunca
    sobre a lista de contratos; validate só roda depois de todos os blocos."""
    dag = dagbag.dags[DAG_ID]
    ingerir = dag.get_task("ingest_contracts")
    assert ingerir.upstream_task_ids == {"get_contract_blocks"}
    # Task decorada guarda o mapeamento em op_kwargs_expand_input.
    mapeado = ingerir.op_kwargs_expand_input.value
    assert set(mapeado) == {"contrato_ids"}
    assert mapeado["contrato_ids"].operator.task_id == "get_contract_blocks"
    assert dag.get_task("validate").upstream_task_ids == {"ingest_contracts"}


# --- get_contract_blocks: escopo, limite local e particionamento -------------


def test_le_contratos_pelo_escopo_de_orgaos(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercita o helper real de escopo: une ativos e inativos, filtra pelo
    órgão e remove o id que aparece nas duas raws (contrato inativado entre
    duas varreduras)."""
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


# --- ingest_contracts: chamada, escrita e validação do lote ------------------


def test_ocorrencia_e_gravada_sem_alterar_o_payload(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A raw recebe a ocorrência como veio: nenhum campo renomeado, achatado ou
    descartado, e `arquivos` continua lista de objetos até o write_raw."""
    ingerir = task(dagbag, "ingest_contracts")
    payload = [ocorrencia()]
    original = copy.deepcopy(payload)
    cliente, escrita = preparar_ingestao(ingerir, monkeypatch, [payload, []])

    assert ingerir(["115", "2289"]) == {
        "ocorrencias": 1,
        "contratos_vazios": 1,
        "linhas_repetidas": 0,
    }
    assert cliente.listar_subrecurso.call_args_list == [
        call("115", "ocorrencias"),
        call("2289", "ocorrencias"),
    ]
    escrita.assert_called_once_with(
        "contratos_gov",
        "contrato_ocorrencia",
        original,
        primary_key=["id"],
        run_id=None,
        json_fields=["arquivos"],
    )
    gravado = escrita.call_args.args[2][0]
    assert set(gravado) == CAMPOS_OPENAPI
    assert gravado["arquivos"] == ARQUIVOS_OPENAPI


def test_cada_contrato_grava_com_run_id_proprio(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    ingerir = task(dagbag, "ingest_contracts")
    _, escrita = preparar_ingestao(
        ingerir,
        monkeypatch,
        [[ocorrencia(115, id=20)], [ocorrencia(2289, id=21)]],
    )
    monkeypatch.setitem(
        ingerir.__globals__,
        "get_current_context",
        lambda: {"run_id": "manual__2026-11-16"},
    )

    ingerir(["115", "2289"])

    assert [chamada.kwargs["run_id"] for chamada in escrita.call_args_list] == [
        "manual__2026-11-16__contrato_115",
        "manual__2026-11-16__contrato_2289",
    ]


def test_contratos_sem_ocorrencia_nao_geram_escrita(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """200 [] é a resposta comum: conta como vazio e não derruba o bloco."""
    ingerir = task(dagbag, "ingest_contracts")
    _, escrita = preparar_ingestao(ingerir, monkeypatch, [[], []])

    assert ingerir(["115", "2289"]) == {
        "ocorrencias": 0,
        "contratos_vazios": 2,
        "linhas_repetidas": 0,
    }
    escrita.assert_not_called()


def test_copia_identica_e_descartada_com_aviso(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """O ON CONFLICT do warehouse recusa a mesma chave duas vezes no mesmo
    comando; a cópia exata sai antes da escrita e é contada."""
    ingerir = task(dagbag, "ingest_contracts")
    _, escrita = preparar_ingestao(ingerir, monkeypatch, [[ocorrencia(), ocorrencia()]])

    with caplog.at_level(logging.WARNING):
        resultado = ingerir(["115"])

    assert resultado == {"ocorrencias": 1, "contratos_vazios": 0, "linhas_repetidas": 1}
    assert escrita.call_args.args[2] == [ocorrencia()]
    assert "descartada(s) antes da escrita" in caplog.text


@pytest.mark.parametrize(
    ("payload", "mensagem"),
    [
        ([ocorrencia(99999)], "contrato_id divergente da URL"),
        (
            [{k: v for k, v in OCORRENCIA_OPENAPI.items() if k != "contrato_id"}],
            "contrato_id divergente da URL",
        ),
        (
            [{k: v for k, v in OCORRENCIA_OPENAPI.items() if k != "id"}],
            "ocorrência sem id",
        ),
        (
            [ocorrencia(situacao="Atendida"), ocorrencia(situacao="Pendente")],
            "repetido no lote com conteúdo diferente",
        ),
    ],
    ids=["contrato_id_divergente", "sem_contrato_id", "sem_id", "id_conflitante"],
)
def test_lote_invalido_falha_antes_da_escrita(
    dagbag: DagBag,
    monkeypatch: pytest.MonkeyPatch,
    payload: list[dict],
    mensagem: str,
) -> None:
    """O cliente é mockado, então estes casos exercitam só a segunda barreira
    (_validar_lote); a primeira, no cliente, é coberta em
    test_cliente_contratos_gov.py. Na API real, linha sem contrato_id recebe o
    da URL no cliente; aqui ela chega crua para provar que a DAG não grava uma
    linha sem vínculo verificável."""
    ingerir = task(dagbag, "ingest_contracts")
    _, escrita = preparar_ingestao(ingerir, monkeypatch, [payload])

    with pytest.raises(RuntimeError, match=mensagem):
        ingerir(["115"])

    escrita.assert_not_called()


# --- backends da zona raw (ADR-0021) -----------------------------------------


def test_object_storage_mantem_arquivos_por_contrato_e_execucao(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = lambda contrato_id, _: [
        ocorrencia(int(contrato_id), id=int(contrato_id) * 10)
    ]
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

    assert ingerir(["115", "2289"])["ocorrencias"] == 2
    # Retry da mesma DagRun também deve preservar os arquivos anteriores.
    assert ingerir(["115", "2289"])["ocorrencias"] == 2
    run_id[0] = "segundo"
    assert ingerir(["115", "2289"])["ocorrencias"] == 2

    arquivos = sorted(tmp_path.rglob("*.parquet"))
    assert len(arquivos) == 6
    assert all("contratos_gov/contrato_ocorrencia/" in str(a) for a in arquivos)
    identificadores = [arquivo.stem.split("--lote-")[0] for arquivo in arquivos]
    assert sorted(identificadores) == sorted(
        ["primeiro__contrato_115", "primeiro__contrato_2289"] * 2
        + ["segundo__contrato_115", "segundo__contrato_2289"]
    )
    for arquivo in arquivos:
        registro = pl.read_parquet(arquivo).to_dicts()[0]
        assert str(registro["contrato_id"]) in arquivo.stem
        assert registro["dt_ingest"]
        # O Parquet guarda `arquivos` como lista de struct: a struct une as
        # chaves dos elementos (arquivo_1, arquivo_2) e preenche a ausente com
        # null. Sem os nulos, cada elemento é o objeto que a fonte mandou.
        assert [
            {chave: url for chave, url in elemento.items() if url is not None}
            for elemento in registro["arquivos"]
        ] == ARQUIVOS_OPENAPI


def test_warehouse_grava_arquivos_como_json_com_chave_id(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No backend warehouse, a mesma chamada vira upsert por `id` em
    contratos_gov.raw_contrato_ocorrencia, com `arquivos` em JSON recuperável e
    o payload em memória intacto."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.return_value = [ocorrencia()]
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
        assert ingerir(["115"])["ocorrencias"] == 1

    registros, tabela = banco.insert_data.call_args.args
    kwargs = banco.insert_data.call_args.kwargs
    assert tabela == "raw_contrato_ocorrencia"
    assert kwargs["schema"] == "contratos_gov"
    assert kwargs["primary_key"] == kwargs["conflict_fields"] == ["id"]
    assert json.loads(registros[0]["arquivos"]) == ARQUIVOS_OPENAPI
    assert registros[0]["dt_ingest"]
    assert set(registros[0]) == CAMPOS_OPENAPI | {"dt_ingest"}


# --- validate ----------------------------------------------------------------


def test_validacao_soma_os_blocos(
    dagbag: DagBag, caplog: pytest.LogCaptureFixture
) -> None:
    validar = task(dagbag, "validate")

    with caplog.at_level(logging.WARNING):
        total = validar(
            [
                {"ocorrencias": 3, "contratos_vazios": 1, "linhas_repetidas": 2},
                {"ocorrencias": 2, "contratos_vazios": 4, "linhas_repetidas": 0},
            ]
        )

    assert total == 5
    assert "nada foi gravado" not in caplog.text


def test_execucao_sem_nenhuma_ocorrencia_avisa_e_nao_fabrica_dado(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Fase 4: zero é o resultado esperado enquanto a fonte só devolver []. A
    execução termina com sucesso e avisa que a raw pode não existir — sem
    gravar linha vazia nem criar tabela para "fazer a Bronze existir"."""
    validar = task(dagbag, "validate")
    escrita = MagicMock()
    monkeypatch.setitem(validar.__globals__, "write_raw", escrita)

    with caplog.at_level(logging.WARNING):
        total = validar(
            [
                {"ocorrencias": 0, "contratos_vazios": 25, "linhas_repetidas": 0},
                {"ocorrencias": 0, "contratos_vazios": 25, "linhas_repetidas": 0},
            ]
        )

    assert total == 0
    escrita.assert_not_called()
    avisos = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(avisos) == 1
    assert "Nenhum dos 50 contrato(s) consultados tem ocorrência" in avisos[0].message
    assert "contratos_gov.raw_contrato_ocorrencia ainda não existe" in avisos[0].message
