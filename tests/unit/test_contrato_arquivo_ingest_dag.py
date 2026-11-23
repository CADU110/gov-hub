"""Comportamento da ingestão dos arquivos por contrato (issue #27)."""

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
    / "contrato_arquivo_ingest_dag.py"
)
DAG_ID = "contrato_arquivo_ingest_dag"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FILE), include_examples=False)


def task(dagbag: DagBag, task_id: str):
    return dagbag.dags[DAG_ID].get_task(task_id).python_callable


def anexo(numero: int, contrato_id: str = "2289") -> dict:
    """Anexo no formato da origem: só metadados e os caminhos para o binário."""
    return {
        "id": numero,
        "contrato_id": contrato_id,
        "tipo": "Contrato",
        "descricao": f"Termo aditivo {numero:02d}",
        "path_arquivo": f"contratos/2024/{contrato_id}/anexo-{numero}.pdf",
        "link_sei": f"https://sei.exemplo.gov.br/documento/{numero}",
        "sequencial_documento": numero,
        "origem": "Upload",
        "usuario_id": 77,
    }


def test_dag_carrega_no_horario_do_escalonamento(dagbag: DagBag) -> None:
    """15:30 de sábado: oitavo sub-recurso na ordem das fases da issue #15,
    meia hora depois da garantia (docs/notas/contratos-gov-ingestao.md)."""
    assert dagbag.import_errors == {}, dagbag.import_errors
    assert dagbag.dags[DAG_ID].timetable.expression == "30 15 * * 6"


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


def test_anexos_sao_gravados_sem_alterar_o_payload(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Os nove campos chegam à raw como vieram, caminho e link inclusive."""
    ingerir = task(dagbag, "ingest_contracts")
    payload = [anexo(1), anexo(2)]
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [payload, []]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    assert ingerir(["2289", "2290"]) == {"arquivos": 2, "contratos_vazios": 1}
    assert cliente.listar_subrecurso.call_args_list == [
        call("2289", "arquivos"),
        call("2290", "arquivos"),
    ]
    escrita.assert_called_once_with(
        "contratos_gov",
        "contrato_arquivo",
        payload,
        primary_key=["id"],
        run_id=None,
    )
    gravado = escrita.call_args.args[2][0]
    assert gravado["path_arquivo"] == "contratos/2024/2289/anexo-1.pdf"
    assert gravado["link_sei"] == "https://sei.exemplo.gov.br/documento/1"


def test_so_metadados_o_binario_nao_e_baixado(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`path_arquivo` e `link_sei` são texto na raw: baixar o anexo está fora
    do escopo desta ingestão, então a única chamada ao cliente é a listagem."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.return_value = [anexo(1)]
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", MagicMock())

    ingerir(["2289"])

    assert [nome for nome, _, _ in cliente.mock_calls] == ["listar_subrecurso"]


def test_contrato_sem_anexo_nao_grava_e_segue(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Contrato sem anexo é comum (a amostra vai de 0 a 11): conta como vazio
    e não interrompe o bloco."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [[], [anexo(1, "2290")]]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    assert ingerir(["2289", "2290"]) == {"arquivos": 1, "contratos_vazios": 1}
    assert escrita.call_count == 1


def test_cada_contrato_tem_arquivo_proprio_no_object_storage(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """O backend append-only não sobrescreve outro contrato nem outro run."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = lambda contrato_id, _: [anexo(1, contrato_id)]
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

    assert ingerir(["2289", "2290"])["arquivos"] == 2
    run_id[0] = "segundo"
    assert ingerir(["2289", "2290"])["arquivos"] == 2

    arquivos = sorted(tmp_path.rglob("*.parquet"))
    # O backend acrescenta `--lote-<hash>` ao nome do arquivo para que dois lotes
    # da mesma run não se sobrescrevam. O que este teste fixa é o par run +
    # contrato, um arquivo para cada, então o sufixo do lote sai da comparação.
    assert len(arquivos) == 4
    assert {arquivo.stem.split("--lote-")[0] for arquivo in arquivos} == {
        "primeiro__contrato_2289",
        "primeiro__contrato_2290",
        "segundo__contrato_2289",
        "segundo__contrato_2290",
    }
    registro = pl.read_parquet(arquivos[0]).to_dicts()[0]
    assert registro["link_sei"].startswith("https://")
    assert registro["dt_ingest"]


@pytest.mark.parametrize(
    ("payload", "motivo"),
    [
        ([{"id": 1, "contrato_id": "outro"}], "contrato_id divergente da URL"),
        ([{"contrato_id": "2289"}], "anexo sem id"),
        ([anexo(1), anexo(1)], "mesmo id duas vezes no lote"),
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
    contrato_id divergente apontaria o anexo para outro contrato."""
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
                {"arquivos": 11, "contratos_vazios": 4},
                {"arquivos": 3, "contratos_vazios": 0},
            ]
        )
        == 14
    )
