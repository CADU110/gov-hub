"""Comportamento da ingestão dos empenhos por contrato (issue #22)."""

from pathlib import Path
from unittest.mock import MagicMock, call

import pytest
from airflow.models import DagBag

pytestmark = pytest.mark.unit

DAG_FILE = (
    Path(__file__).resolve().parents[2]
    / "airflow"
    / "dags"
    / "data_ingest"
    / "contratos_gov"
    / "contrato_empenho_ingest_dag.py"
)
DAG_ID = "contrato_empenho_ingest_dag"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FILE), include_examples=False)


def task(dagbag: DagBag, task_id: str):
    return dagbag.dags[DAG_ID].get_task(task_id).python_callable


def test_dag_carrega_com_agenda_depois_dos_cabecalhos(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, dagbag.import_errors
    assert dagbag.dags[DAG_ID].timetable.expression == "0 13 * * 6"


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


def test_empenhos_sao_gravados_sem_alterar_o_payload(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    ingerir = task(dagbag, "ingest_contracts")
    payload = [
        {
            "id": 7,
            "contrato_id": "2289",
            "credor": "CNPJ - NOME",
            "credor_obj": {"nome": "NOME"},
            "links": {"documento_pagamento": "https://exemplo.invalid/7"},
        }
    ]
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [payload, []]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    assert ingerir(["2289", "2290"]) == {
        "empenhos": 1,
        "contratos_vazios": 1,
        "linhas_repetidas": 0,
    }
    assert cliente.listar_subrecurso.call_args_list == [
        call("2289", "empenhos"),
        call("2290", "empenhos"),
    ]
    escrita.assert_called_once_with(
        "contratos_gov",
        "contrato_empenho",
        payload,
        primary_key=["contrato_id", "id"],
        run_id=None,
        json_fields=["credor_obj", "links"],
    )


def test_mesmo_empenho_em_dois_contratos_e_gravado_nos_dois(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [
        [{"id": 7, "contrato_id": "2289"}],
        [{"id": 7, "contrato_id": "2290"}],
    ]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    assert ingerir(["2289", "2290"])["empenhos"] == 2
    assert [chamada.args[2] for chamada in escrita.call_args_list] == [
        [{"id": 7, "contrato_id": "2289"}],
        [{"id": 7, "contrato_id": "2290"}],
    ]
    assert all(
        chamada.kwargs["primary_key"] == ["contrato_id", "id"]
        for chamada in escrita.call_args_list
    )


def test_linha_identica_repetida_no_contrato_e_descartada(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    ingerir = task(dagbag, "ingest_contracts")
    repetida = {"id": 7, "contrato_id": "93055", "links": {"documento": "a"}}
    cliente = MagicMock()
    cliente.listar_subrecurso.return_value = [
        repetida,
        {"id": 8, "contrato_id": "93055"},
        dict(repetida),
    ]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    assert ingerir(["93055"]) == {
        "empenhos": 2,
        "contratos_vazios": 0,
        "linhas_repetidas": 1,
    }
    assert escrita.call_args.args[2] == [repetida, {"id": 8, "contrato_id": "93055"}]


def test_cada_contrato_tem_arquivo_proprio_no_object_storage(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [
        [{"id": 7, "contrato_id": "2289"}],
        [{"id": 8, "contrato_id": "2290"}],
    ]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)
    monkeypatch.setitem(
        ingerir.__globals__,
        "get_current_context",
        lambda: {"run_id": "manual__2026-10-08"},
    )

    assert ingerir(["2289", "2290"]) == {
        "empenhos": 2,
        "contratos_vazios": 0,
        "linhas_repetidas": 0,
    }
    assert [chamada.kwargs["run_id"] for chamada in escrita.call_args_list] == [
        "manual__2026-10-08__contrato_2289",
        "manual__2026-10-08__contrato_2290",
    ]


@pytest.mark.parametrize(
    "payload",
    [
        [{"id": 7, "contrato_id": "outro"}],
        [
            {"id": 7, "contrato_id": "2289", "empenhado": "1,00"},
            {"id": 7, "contrato_id": "2289", "empenhado": "2,00"},
        ],
        [{"contrato_id": "2289"}],
    ],
)
def test_lote_invalido_falha_antes_da_escrita(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, payload: list[dict]
) -> None:
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
                {"empenhos": 3, "contratos_vazios": 1, "linhas_repetidas": 2},
                {"empenhos": 2, "contratos_vazios": 0, "linhas_repetidas": 0},
            ]
        )
        == 5
    )
