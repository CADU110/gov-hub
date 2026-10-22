"""Comportamento da ingestão do cronograma por contrato (issue #24)."""

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
    / "contrato_cronograma_ingest_dag.py"
)
DAG_ID = "contrato_cronograma_ingest_dag"

# O objeto do termo vem inteiro em `observacao`: o teste usa um texto longo
# para flagrar truncamento, que apagaria o que distingue uma parcela da outra.
OBSERVACAO_LONGA = (
    "Contratação de serviços continuados de limpeza e conservação predial, "
    "com fornecimento de mão de obra, materiais e equipamentos, conforme "
    "termo de referência anexo ao instrumento contratual. " * 4
)


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FILE), include_examples=False)


def task(dagbag: DagBag, task_id: str):
    return dagbag.dags[DAG_ID].get_task(task_id).python_callable


def parcela(numero: int, contrato_id: str = "2289") -> dict:
    """Parcela no formato da origem: valor em texto pt-BR, mês e ano separados."""
    return {
        "id": numero,
        "contrato_id": contrato_id,
        "tipo": "Termo Aditivo",
        "numero": "00001/2024",
        "mesref": (numero % 12) + 1,
        "anoref": 2024,
        "vencimento": "2024-06-15",
        "valor": "5.335.214,10",
        "observacao": OBSERVACAO_LONGA,
    }


def test_dag_carrega_no_horario_do_escalonamento(dagbag: DagBag) -> None:
    """14:00 de sábado: a janela dos sub-recursos começa às 12:00 e escalona de
    30 em 30 minutos na ordem das fases (docs/notas/contratos-gov-ingestao.md)."""
    assert dagbag.import_errors == {}, dagbag.import_errors
    assert dagbag.dags[DAG_ID].timetable.expression == "0 14 * * 6"


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


def test_parcelas_sao_gravadas_sem_alterar_o_payload(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O valor continua em texto pt-BR e a observação inteira: converter e
    truncar são conta da Silver, não da ingestão."""
    ingerir = task(dagbag, "ingest_contracts")
    payload = [parcela(1), parcela(2)]
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [payload, []]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    assert ingerir(["2289", "2290"]) == {"parcelas": 2, "contratos_vazios": 1}
    assert cliente.listar_subrecurso.call_args_list == [
        call("2289", "cronograma"),
        call("2290", "cronograma"),
    ]
    escrita.assert_called_once_with(
        "contratos_gov",
        "contrato_cronograma",
        payload,
        primary_key=["id"],
        run_id=None,
    )
    gravado = escrita.call_args.args[2][0]
    assert gravado["valor"] == "5.335.214,10"
    assert gravado["observacao"] == OBSERVACAO_LONGA


def test_contrato_sem_cronograma_nao_grava_e_segue(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cronograma vazio é comum (a amostra vai de 0 a 97 parcelas): conta como
    vazio e não interrompe o bloco."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [[], [parcela(1, "2290")]]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    assert ingerir(["2289", "2290"]) == {"parcelas": 1, "contratos_vazios": 1}
    assert escrita.call_count == 1


def test_contrato_longo_vai_numa_escrita_so(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Uma escrita por contrato, não por parcela: o maior da amostra tem 97."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.return_value = [parcela(i) for i in range(1, 98)]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    assert ingerir(["2289"]) == {"parcelas": 97, "contratos_vazios": 0}
    assert escrita.call_count == 1
    assert len(escrita.call_args.args[2]) == 97


def test_cada_contrato_tem_arquivo_proprio_no_object_storage(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """O backend append-only não sobrescreve outro contrato nem outro run."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = lambda contrato_id, _: [
        parcela(1, contrato_id)
    ]
    run_id = ["primeiro"]
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
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

    assert ingerir(["2289", "2290"])["parcelas"] == 2
    run_id[0] = "segundo"
    assert ingerir(["2289", "2290"])["parcelas"] == 2

    arquivos = sorted(tmp_path.rglob("*.parquet"))
    assert {arquivo.stem for arquivo in arquivos} == {
        "primeiro__contrato_2289",
        "primeiro__contrato_2290",
        "segundo__contrato_2289",
        "segundo__contrato_2290",
    }
    registro = pl.read_parquet(arquivos[0]).to_dicts()[0]
    assert registro["observacao"] == OBSERVACAO_LONGA
    assert registro["dt_ingest"]


@pytest.mark.parametrize(
    ("payload", "motivo"),
    [
        ([{"id": 1, "contrato_id": "outro"}], "contrato_id divergente da URL"),
        ([{"contrato_id": "2289"}], "parcela sem id"),
        ([parcela(1), parcela(1)], "mesmo id duas vezes no lote"),
    ],
    ids=["contrato_id_divergente", "sem_id", "id_repetido"],
)
def test_lote_invalido_falha_antes_da_escrita(
    dagbag: DagBag,
    monkeypatch: pytest.MonkeyPatch,
    payload: list[dict],
    motivo: str,
) -> None:
    """A chave primária é só `id`: id repetido quebra o ON CONFLICT, e
    contrato_id divergente apontaria a parcela para outro contrato."""
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
                {"parcelas": 97, "contratos_vazios": 3},
                {"parcelas": 12, "contratos_vazios": 0},
            ]
        )
        == 109
    )
