"""Comportamento da ingestão dos itens por contrato (issue #21)."""

from pathlib import Path
from unittest.mock import MagicMock, call

import fsspec
import polars as pl
import pytest
from airflow.models import DagBag

import landing_zone

pytestmark = pytest.mark.unit

DAG_FILE = (
    Path(__file__).resolve().parents[2]
    / "airflow"
    / "dags"
    / "data_ingest"
    / "contratos_gov"
    / "contrato_item_ingest_dag.py"
)
DAG_ID = "contrato_item_ingest_dag"

# data_inicio_item é objeto (serialização do Carbon/PHP) e historico_item é
# lista aninhada: os dois vão para json_fields e são explodidos na Silver.
DATA_INICIO = {
    "date": "2024-03-01 00:00:00.000000",
    "timezone_type": 3,
    "timezone": "UTC",
}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FILE), include_examples=False)


def task(dagbag: DagBag, task_id: str):
    return dagbag.dags[DAG_ID].get_task(task_id).python_callable


def test_dag_carrega_no_horario_do_escalonamento(dagbag: DagBag) -> None:
    """12:30 de sábado: a janela dos sub-recursos começa às 12:00 e escalona de
    30 em 30 minutos na ordem das fases (docs/notas/contratos-gov-ingestao.md)."""
    assert dagbag.import_errors == {}, dagbag.import_errors
    assert dagbag.dags[DAG_ID].timetable.expression == "30 12 * * 6"


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


def test_itens_sao_gravados_sem_alterar_o_payload(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A raw recebe o item como veio: data em objeto, histórico em lista."""
    ingerir = task(dagbag, "ingest_contracts")
    payload = [
        {
            "id": 31,
            "contrato_id": "2289",
            "tipo_id": 1,
            "catmatseritem_id": 4505,
            "descricao_complementar": "SERVICO DE LIMPEZA",
            "quantidade": "12.0000",
            "valorunitario": "1.000,00",
            "data_inicio_item": DATA_INICIO,
            "historico_item": [{"data_termo": "2024-06-01", "quantidade": "10.0000"}],
        }
    ]
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [payload, []]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    assert ingerir(["2289", "2290"]) == {"itens": 1, "contratos_vazios": 1}
    assert cliente.listar_subrecurso.call_args_list == [
        call("2289", "itens"),
        call("2290", "itens"),
    ]
    escrita.assert_called_once_with(
        "contratos_gov",
        "contrato_item",
        payload,
        primary_key=["id"],
        run_id=None,
        json_fields=["data_inicio_item", "historico_item"],
    )


def test_contrato_sem_item_nao_grava_e_segue(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """200 [] é resposta legítima: conta como vazio e não derruba o bloco."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [[], [{"id": 32, "contrato_id": "2290"}]]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    assert ingerir(["2289", "2290"]) == {"itens": 1, "contratos_vazios": 1}
    assert escrita.call_count == 1


def test_cada_contrato_tem_arquivo_proprio_no_object_storage(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [
        [{"id": 31, "contrato_id": "2289"}],
        [{"id": 32, "contrato_id": "2290"}],
    ]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)
    monkeypatch.setitem(
        ingerir.__globals__,
        "get_current_context",
        lambda: {"run_id": "manual__2026-10-08"},
    )

    ingerir(["2289", "2290"])

    assert [chamada.kwargs["run_id"] for chamada in escrita.call_args_list] == [
        "manual__2026-10-08__contrato_2289",
        "manual__2026-10-08__contrato_2290",
    ]


def test_object_storage_preserva_data_e_historico_aninhados(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Sem amostragem de schema, o objeto da data e a lista do histórico
    sobrevivem ao Parquet."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = lambda contrato_id, _: [
        {
            "id": 31,
            "contrato_id": contrato_id,
            "data_inicio_item": DATA_INICIO,
            "historico_item": [{"data_termo": "2024-06-01"}],
        }
    ]
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", landing_zone.write_raw)
    monkeypatch.setitem(
        ingerir.__globals__, "get_current_context", lambda: {"run_id": "run"}
    )
    monkeypatch.setattr(
        landing_zone,
        "get_storage_fs",
        lambda: fsspec.filesystem("file", auto_mkdir=True),
    )
    monkeypatch.setattr(landing_zone, "get_bucket", lambda: str(tmp_path))
    monkeypatch.setenv("RAW_BACKEND", "object_storage")

    assert ingerir(["2289"])["itens"] == 1

    arquivo = next(tmp_path.rglob("*.parquet"))
    registro = pl.read_parquet(arquivo).to_dicts()[0]
    assert registro["data_inicio_item"]["timezone_type"] == 3
    assert registro["historico_item"][0]["data_termo"] == "2024-06-01"
    assert registro["dt_ingest"]


@pytest.mark.parametrize(
    ("payload", "motivo"),
    [
        ([{"id": 31, "contrato_id": "outro"}], "contrato_id divergente da URL"),
        ([{"contrato_id": "2289"}], "item sem id"),
        (
            [
                {"id": 31, "contrato_id": "2289", "quantidade": "1.0000"},
                {"id": 31, "contrato_id": "2289", "quantidade": "2.0000"},
            ],
            "mesmo id com conteúdo diferente",
        ),
    ],
    ids=["contrato_id_divergente", "sem_id", "id_repetido_divergente"],
)
def test_lote_invalido_falha_antes_da_escrita(
    dagbag: DagBag,
    monkeypatch: pytest.MonkeyPatch,
    payload: list[dict],
    motivo: str,
) -> None:
    """A chave primária é só `id`: id repetido no mesmo comando quebra o
    ON CONFLICT, e contrato_id divergente apontaria o item para outro contrato."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.return_value = payload
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    with pytest.raises(RuntimeError):
        ingerir(["2289"])

    escrita.assert_not_called()


def test_validacao_conta_os_blocos(dagbag: DagBag) -> None:
    validar = task(dagbag, "validate")
    assert (
        validar(
            [
                {"itens": 3, "contratos_vazios": 1},
                {"itens": 2, "contratos_vazios": 0},
            ]
        )
        == 5
    )
