"""Comportamento da ingestão dos terceirizados por contrato.

A API devolveu só listas vazias, então os registros destes testes seguem o
formato de Contratoterceirizado::terceirizadoAPI no código-fonte público do
Comprasnet Contratos (gitlab.com/comprasnet/contratos, `master` em adb9f9ee,
lido em 2026-11-25), e não uma resposta real. Os nomes e documentos são
fictícios.
"""

import ast
import logging
from copy import deepcopy
from http import HTTPStatus
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import fsspec
import polars as pl
import pytest
import yaml
from airflow.models import DagBag

import landing_zone
from cliente_contratos_gov import ClienteContratosGov
from cliente_postgres import ClientPostgresDB
from landing_zone import RawIndisponivel

pytestmark = pytest.mark.unit

RAIZ = Path(__file__).resolve().parents[2]
DAG_FILE = (
    RAIZ
    / "airflow"
    / "dags"
    / "data_ingest"
    / "contratos_gov"
    / "contrato_terceirizado_ingest_dag.py"
)
DAG_ID = "contrato_terceirizado_ingest_dag"

USUARIO = "***.111.111-** FULANO DE TAL"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FILE), include_examples=False)


def task(dagbag: DagBag, task_id: str):
    assert DAG_ID in dagbag.dags, f"{DAG_ID} não carregou: {dagbag.import_errors}"
    return dagbag.dags[DAG_ID].get_task(task_id).python_callable


def terceirizado(numero: int, contrato_id: int = 2289) -> dict:
    return {
        "id": numero,
        "contrato_id": contrato_id,
        "usuario": USUARIO,
        "funcao_id": "Ajudante",
        "descricao_complementar": None,
        "jornada": 44,
        "unidade": "SEDE",
        "salario": "1.200,25",
        "custo": "3.450,00",
        "escolaridade_id": "Ensino médio completo",
        "data_inicio": "2025-01-01",
        "data_fim": None,
        "situacao": "Ativo",
        "aux_transporte": "190,00",
        "vale_alimentacao": "560,00",
        "numero_contrato": "00001/2024",
        "unidade_gestora": "201057",
        "unidade_gestora_nome": "UNIDADE FICTICIA",
        "fornecedor_identificador": "00.000.000/0001-00",
    }


def preparar_ingestao(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, cliente: object
) -> tuple:
    """Devolve `ingest_contracts` e a escrita, com cliente e raw substituídos."""
    ingerir = task(dagbag, "ingest_contracts")
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)
    return ingerir, escrita


# --- Estrutura ---------------------------------------------------------------


def test_dag_carrega_no_horario_do_escalonamento(dagbag: DagBag) -> None:
    """17:00 de sábado: entre preposto (16:30) e despesa acessória (17:30)."""
    assert dagbag.import_errors == {}, dagbag.import_errors
    assert DAG_ID in dagbag.dags
    assert dagbag.dags[DAG_ID].timetable.expression == "0 17 * * 6"
    assert dagbag.dags[DAG_ID].catchup is False


def test_catalogo_aponta_para_a_dag_e_declara_dado_pessoal() -> None:
    catalogo = yaml.safe_load(
        (RAIZ / "catalogo" / "sistemas" / "contratos_gov.yml").read_text()
    )
    entidade = catalogo["entidades"]["contrato_terceirizado"]
    assert entidade["dag"] == DAG_ID
    assert entidade["origem_atual"] == "contratos_gov.raw_contrato_terceirizado"
    assert entidade["chave_primaria"] == ["id"]
    assert entidade["classificacao"] == "pessoal"


def test_bronze_le_a_tabela_raw_da_entidade() -> None:
    fontes = yaml.safe_load(
        (RAIZ / "airflow/dags/dbt/contratos_gov/models/bronze/sources.yml").read_text()
    )
    (fonte,) = [f for f in fontes["sources"] if f["name"] == "contratos_gov"]
    (tabela,) = [t for t in fonte["tables"] if t["name"] == "contrato_terceirizado"]
    assert fonte["schema"] == "contratos_gov"
    assert tabela["identifier"] == "raw_contrato_terceirizado"
    assert tabela["meta"]["dag_ingestao"] == DAG_ID
    assert tabela["meta"]["classificacao"] == "pessoal"


def test_fan_out_expande_blocos_e_valida_no_fim(dagbag: DagBag) -> None:
    """`.expand()` recebe os blocos, nunca a lista de contratos."""
    assert DAG_ID in dagbag.dags, dagbag.import_errors
    dag = dagbag.dags[DAG_ID]
    assert set(dag.task_ids) == {"get_contract_blocks", "ingest_contracts", "validate"}
    ingestao = dag.get_task("ingest_contracts")
    assert ingestao.is_mapped
    assert ingestao.upstream_task_ids == {"get_contract_blocks"}
    assert dag.get_task("validate").upstream_task_ids == {"ingest_contracts"}


def test_dag_nao_importa_cliente_de_banco() -> None:
    """ADR-0011 e ADR-0021; e `dag`/`task` vêm de airflow.sdk."""
    modulos = set()
    for no in ast.walk(ast.parse(DAG_FILE.read_text())):
        if isinstance(no, ast.Import):
            modulos |= {alias.name for alias in no.names}
        elif isinstance(no, ast.ImportFrom):
            modulos.add(no.module)
    proibidos = {
        "cliente_postgres",
        "cliente_sqlserver",
        "postgres_helpers",
        "psycopg2",
        "sqlalchemy",
        "airflow.decorators",
    }
    assert not modulos & proibidos
    assert "airflow.sdk" in modulos


def test_docstring_avisa_que_a_carga_completa_nao_roda_localmente() -> None:
    docstring = ast.get_docstring(ast.parse(DAG_FILE.read_text())) or ""
    assert "INGEST_MAX_CONTRATOS" in docstring
    assert "local.env" in docstring
    assert "homologação" in docstring


def test_limite_de_contratos_esta_definido_no_local_env() -> None:
    linhas = (RAIZ / "local.env").read_text().splitlines()
    (valor,) = [
        linha.split("=")[1]
        for linha in linhas
        if linha.startswith("INGEST_MAX_CONTRATOS")
    ]
    assert int(valor) > 0


# --- Seleção e particionamento dos contratos ---------------------------------


def test_filtra_limita_e_particiona_antes_de_expandir(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    preparar = task(dagbag, "get_contract_blocks")
    escopo = MagicMock(return_value={"46000"})
    ids = MagicMock(return_value=[str(i) for i in range(61)])
    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", escopo)
    monkeypatch.setitem(preparar.__globals__, "ids_contratos_no_escopo", ids)
    monkeypatch.setenv("INGEST_MAX_CONTRATOS", "51")

    blocos = preparar()

    assert [len(bloco) for bloco in blocos] == [25, 25, 1]
    assert [id_ for bloco in blocos for id_ in bloco] == [str(i) for i in range(51)]
    ids.assert_called_once_with({"46000"})
    assert dagbag.dags[DAG_ID].get_task("ingest_contracts").max_active_tis_per_dag == 4


def test_muitos_ids_nao_excedem_o_limite_de_mapeamento(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    preparar = task(dagbag, "get_contract_blocks")
    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", lambda: {"46000"})
    monkeypatch.setitem(
        preparar.__globals__,
        "ids_contratos_no_escopo",
        lambda _: [str(i) for i in range(26000)],
    )
    monkeypatch.delenv("INGEST_MAX_CONTRATOS", raising=False)

    blocos = preparar()

    assert len(blocos) <= 1024
    assert sum(map(len, blocos)) == 26000


def test_limite_local_zerado_falha_antes_de_expandir(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    preparar = task(dagbag, "get_contract_blocks")
    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", lambda: {"46000"})
    monkeypatch.setitem(
        preparar.__globals__, "ids_contratos_no_escopo", lambda _: ["2289", "2290"]
    )
    monkeypatch.setenv("INGEST_MAX_CONTRATOS", "0")

    with pytest.raises(RuntimeError, match="INGEST_MAX_CONTRATOS"):
        preparar()


@pytest.mark.parametrize("ausente", ["contrato_ativo", "contrato_inativo"])
def test_raw_de_contratos_ausente_falha_com_mensagem(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, ausente: str
) -> None:
    """Passa pelo helper de escopo de verdade: só a leitura da raw é falsa."""
    preparar = task(dagbag, "get_contract_blocks")

    def ler_raw(sistema: str, entidade: str, colunas: list[str]) -> list[tuple]:
        if entidade == ausente:
            raise RawIndisponivel(entidade)
        return [(2289, "46000")]

    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", lambda: {"46000"})
    selecionar = preparar.__globals__["ids_contratos_no_escopo"]
    monkeypatch.setitem(selecionar.__globals__, "distinct_raw_rows", ler_raw)

    with pytest.raises(RuntimeError, match=f"{ausente}_ingest_dag"):
        preparar()


def test_une_ativos_e_inativos_so_do_escopo_sem_repetir_contrato(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Contrato nas duas raws é consultado uma vez; órgão de fora, nenhuma."""
    preparar = task(dagbag, "get_contract_blocks")
    raw = {
        "contrato_ativo": [(9, "46000"), (2, "46000"), (100, "25206")],
        "contrato_inativo": [(2, "46000"), (15, "46000")],
    }
    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", lambda: {"46000"})
    selecionar = preparar.__globals__["ids_contratos_no_escopo"]
    monkeypatch.setitem(
        selecionar.__globals__, "distinct_raw_rows", lambda _, entidade, __: raw[entidade]
    )
    monkeypatch.delenv("INGEST_MAX_CONTRATOS", raising=False)

    assert preparar() == [["2", "9", "15"]]


# --- Gravação ----------------------------------------------------------------


def test_terceirizados_sao_gravados_sem_alterar_o_payload(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Valor em texto pt-BR e nulos seguem como vieram: converter é da Silver."""
    payload = [terceirizado(1), terceirizado(2)]
    esperado = deepcopy(payload)
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [payload, []]
    ingerir, escrita = preparar_ingestao(dagbag, monkeypatch, cliente)

    assert ingerir(["2289", "2290"]) == {"terceirizados": 2, "contratos_vazios": 1}
    assert cliente.listar_subrecurso.call_args_list == [
        call("2289", "terceirizados"),
        call("2290", "terceirizados"),
    ]
    escrita.assert_called_once_with(
        "contratos_gov",
        "contrato_terceirizado",
        esperado,
        primary_key=["id"],
        run_id=None,
    )


def test_contrato_sem_terceirizado_nao_grava_e_segue(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [[], [terceirizado(1, 2290)]]
    ingerir, escrita = preparar_ingestao(dagbag, monkeypatch, cliente)

    assert ingerir(["2289", "2290"]) == {"terceirizados": 1, "contratos_vazios": 1}
    assert escrita.call_count == 1


def test_todos_os_contratos_vazios_nao_gravam_nada(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """É o caso de toda a amostra da issue: 200 [] não é erro."""
    cliente = MagicMock()
    cliente.listar_subrecurso.return_value = []
    ingerir, escrita = preparar_ingestao(dagbag, monkeypatch, cliente)

    assert ingerir(["2289", "2290"]) == {"terceirizados": 0, "contratos_vazios": 2}
    escrita.assert_not_called()


def test_cada_contrato_tem_arquivo_proprio_no_object_storage(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """O backend append-only não sobrescreve outro contrato nem outro run."""
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = lambda contrato_id, _: [
        terceirizado(int(contrato_id), int(contrato_id))
    ]
    ingerir, _ = preparar_ingestao(dagbag, monkeypatch, cliente)
    run_id = ["primeiro"]
    monkeypatch.setitem(ingerir.__globals__, "write_raw", landing_zone.write_raw)
    monkeypatch.setitem(
        ingerir.__globals__, "get_current_context", lambda: {"run_id": run_id[0]}
    )
    monkeypatch.setattr(
        landing_zone,
        "get_storage_fs",
        lambda: fsspec.filesystem("file", auto_mkdir=True),
    )
    monkeypatch.setattr(landing_zone, "get_bucket", lambda: str(tmp_path))
    monkeypatch.setenv("RAW_BACKEND", "object_storage")

    assert ingerir(["2289", "2290"])["terceirizados"] == 2
    run_id[0] = "segundo"
    assert ingerir(["2289", "2290"])["terceirizados"] == 2

    arquivos = sorted(tmp_path.rglob("*.parquet"))
    assert len(arquivos) == 4
    assert {arquivo.stem.split("--lote-")[0] for arquivo in arquivos} == {
        "primeiro__contrato_2289",
        "primeiro__contrato_2290",
        "segundo__contrato_2289",
        "segundo__contrato_2290",
    }
    for arquivo in arquivos:
        registro = pl.read_parquet(arquivo).to_dicts()[0]
        assert registro["contrato_id"] in {2289, 2290}
        assert registro["salario"] == "1.200,25"
        assert registro["dt_ingest"]


def ingerir_no_warehouse(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, payload: list[dict]
) -> tuple[str, list[dict]]:
    """Passa por write_raw e ClientPostgresDB reais; devolve o SQL e as linhas."""
    cliente = MagicMock()
    cliente.listar_subrecurso.return_value = payload
    ingerir, _ = preparar_ingestao(dagbag, monkeypatch, cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", landing_zone.write_raw)
    monkeypatch.setenv("RAW_BACKEND", "warehouse")
    for metodo in (
        "_connect",
        "create_table_if_not_exists",
        "alter_table",
        "ensure_unique_constraint",
    ):
        monkeypatch.setattr(ClientPostgresDB, metodo, MagicMock())
    with (
        patch.dict(
            "sys.modules",
            {"postgres_helpers": MagicMock(get_postgres_conn=lambda _: "conn")},
        ),
        patch("cliente_postgres.psycopg2.extras.execute_values") as executar,
    ):
        ingerir(["2289"])

    _, sql, valores = executar.call_args.args
    colunas = sql.split("(", 1)[1].split(")", 1)[0].split(", ")
    return sql, [dict(zip(colunas, linha, strict=True)) for linha in valores]


def test_warehouse_faz_upsert_por_id_com_dt_ingest(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    sql, linhas = ingerir_no_warehouse(dagbag, monkeypatch, [terceirizado(1)])

    assert sql.startswith("INSERT INTO contratos_gov.raw_contrato_terceirizado ")
    assert "ON CONFLICT (id) DO UPDATE" in sql
    assert linhas[0]["contrato_id"] == 2289
    assert linhas[0]["dt_ingest"]


def test_warehouse_preserva_inteiro_e_nulo(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Com um nulo no lote, o pandas transforma 44 em 44.0 e o nulo em 'NaN'."""
    _, linhas = ingerir_no_warehouse(
        dagbag,
        monkeypatch,
        [
            {**terceirizado(1), "descricao_complementar": "Almoxarifado"},
            {**terceirizado(2), "jornada": None},
        ],
    )

    assert [str(linha["jornada"]) for linha in linhas] == ["44", "None"]
    assert linhas[1]["descricao_complementar"] is None


# --- Vínculo com o contrato e chave primária ---------------------------------


def ingestao_com_cliente_real(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, resposta: list[dict]
) -> tuple:
    """Só o HTTP é falso: um mock de listar_subrecurso esconderia o caso em que
    o cliente sobrescreve o contrato_id do payload com o da URL."""
    cliente = ClienteContratosGov(request_delay=0)
    http = MagicMock(return_value=(HTTPStatus.OK, resposta))
    monkeypatch.setattr(cliente, "request", http)
    return *preparar_ingestao(dagbag, monkeypatch, cliente), http


def test_contrato_id_da_url_entra_em_toda_linha(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    sem_vinculo = {k: v for k, v in terceirizado(1).items() if k != "contrato_id"}
    ingerir, escrita, http = ingestao_com_cliente_real(
        dagbag, monkeypatch, [sem_vinculo, terceirizado(2)]
    )

    assert ingerir(["2289"])["terceirizados"] == 2

    http.assert_called_once_with("GET", "/api/contrato/2289/terceirizados")
    gravados = escrita.call_args.args[2]
    # Como texto: a origem devolve int e o id da URL vem da raw como str.
    assert [str(r["contrato_id"]) for r in gravados] == ["2289", "2289"]


def test_contrato_id_divergente_da_url_falha_antes_da_escrita(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    ingerir, escrita, _ = ingestao_com_cliente_real(
        dagbag, monkeypatch, [terceirizado(1), terceirizado(2, 9999)]
    )

    with pytest.raises(RuntimeError, match="contrato_id divergente"):
        ingerir(["2289"])

    escrita.assert_not_called()


@pytest.mark.parametrize(
    ("payload", "mensagem"),
    [
        ([terceirizado(1, 9999)], "contrato_id divergente"),
        ([{**terceirizado(1), "id": None}], "sem id"),
        ([terceirizado(1), terceirizado(1)], "repetido"),
        ([terceirizado(1), {**terceirizado(1), "id": "1"}], "repetido"),
    ],
    ids=["contrato_id_divergente", "sem_id", "id_repetido", "id_repetido_em_texto"],
)
def test_lote_invalido_falha_antes_da_escrita(
    dagbag: DagBag,
    monkeypatch: pytest.MonkeyPatch,
    payload: list[dict],
    mensagem: str,
) -> None:
    """A chave é só `id`: repetido, quebraria o ON CONFLICT do warehouse."""
    cliente = MagicMock()
    cliente.listar_subrecurso.return_value = payload
    ingerir, escrita = preparar_ingestao(dagbag, monkeypatch, cliente)

    with pytest.raises(RuntimeError, match=mensagem) as erro:
        ingerir(["2289"])

    escrita.assert_not_called()
    assert "FULANO" not in str(erro.value), "dado pessoal na mensagem de erro"


def test_falha_da_api_nao_vira_contrato_vazio(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = Exception("API indisponível")
    ingerir, escrita = preparar_ingestao(dagbag, monkeypatch, cliente)

    with pytest.raises(Exception, match="API indisponível"):
        ingerir(["2289"])

    escrita.assert_not_called()


def test_log_da_ingestao_nao_expoe_dado_pessoal(
    dagbag: DagBag,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log de task é legível por quem não tem acesso à raw (ADR-0020)."""
    cliente = MagicMock()
    cliente.listar_subrecurso.return_value = [terceirizado(1)]
    ingerir, _ = preparar_ingestao(dagbag, monkeypatch, cliente)

    with caplog.at_level(logging.DEBUG):
        ingerir(["2289"])

    assert "FULANO" not in caplog.text
    assert "111.111" not in caplog.text
    assert "1.200,25" not in caplog.text


# --- Validação ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("resultados", "total"),
    [
        (
            [
                {"terceirizados": 20, "contratos_vazios": 5},
                {"terceirizados": 1, "contratos_vazios": 24},
            ],
            21,
        ),
        ([{"terceirizados": 0, "contratos_vazios": 25}], 0),
    ],
    ids=["soma_os_blocos", "total_zero"],
)
def test_validacao_conta_os_blocos(
    dagbag: DagBag, resultados: list[dict], total: int
) -> None:
    assert task(dagbag, "validate")(resultados) == total
