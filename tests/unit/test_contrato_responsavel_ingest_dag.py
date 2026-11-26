"""Comportamento da ingestão dos responsáveis por contrato (issue #28).

Fiscais e gestores designados para o contrato. Todo dado pessoal aqui é
inventado: nome fictício, CPF já mascarado como a origem entrega. Nada de
amostra real da API neste arquivo.
"""

from pathlib import Path
from unittest.mock import MagicMock, call

import fsspec
import polars as pl
import pytest
import yaml
from airflow.models import DagBag

import landing_zone

pytestmark = pytest.mark.unit

RAIZ = Path(__file__).resolve().parents[2]
DAG_FILE = (
    RAIZ
    / "airflow"
    / "dags"
    / "data_ingest"
    / "contratos_gov"
    / "contrato_responsavel_ingest_dag.py"
)
DAG_ID = "contrato_responsavel_ingest_dag"
NOME_FICTICIO = "FULANA DE TAL EXEMPLO"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FILE), include_examples=False)


def task(dagbag: DagBag, task_id: str):
    return dagbag.dags[DAG_ID].get_task(task_id).python_callable


def responsavel(numero: int, contrato_id: int = 2289) -> dict:
    """Responsável no formato de Contratoresponsavel::responsavelAPI do código
    público do Comprasnet Contratos: nove campos planos, contrato_id inteiro,
    funcao_id e instalacao_id como descrição em texto (instalação ausente vem
    como texto vazio) e situacao como 'Ativo'/'Inativo'. O dado pessoal está
    em usuario (CPF mascarado e nome completo)."""
    return {
        "id": numero,
        "contrato_id": contrato_id,
        "usuario": f"***.000.000-** - {NOME_FICTICIO}",
        "funcao_id": "Fiscal Técnico",
        "instalacao_id": "",
        "portaria": f"Portaria {numero:03d}/2024",
        "situacao": "Ativo",
        "data_inicio": "2024-01-02",
        "data_fim": None,
    }


def test_dag_carrega_no_horario_do_escalonamento(dagbag: DagBag) -> None:
    """16:00 de sábado: meia hora antes de contrato_preposto, na ordem das
    fases da issue #15 (docs/notas/contratos-gov-ingestao.md)."""
    assert dagbag.import_errors == {}, dagbag.import_errors
    assert dagbag.dags[DAG_ID].timetable.expression == "0 16 * * 6"


def test_catalogo_classifica_a_entidade_como_pessoal() -> None:
    """ADR-0013/0020: é a classificação que impede a publicação de expor nome e
    CPF mascarado a consumidor de nível menor."""
    catalogo = yaml.safe_load(
        (RAIZ / "catalogo" / "sistemas" / "contratos_gov.yml").read_text()
    )
    entidade = catalogo["entidades"]["contrato_responsavel"]
    assert entidade["classificacao"] in {"pessoal", "pessoal_sensivel"}
    assert entidade["chave_primaria"] == ["id"]
    assert entidade["dag"] == DAG_ID


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


def test_raw_de_contratos_ausente_falha_com_mensagem(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dependência é de ativos e inativos: sem a raw, a mensagem diz qual
    DAG rodar antes, em vez de seguir com lista vazia."""
    preparar = task(dagbag, "get_contract_blocks")
    monkeypatch.setitem(preparar.__globals__, "orgaos_no_escopo", lambda: {"46000"})

    def sem_raw(_):
        raise RuntimeError(
            "Entidade 'contrato_ativo' ainda não está na zona raw. "
            "Execute contrato_ativo_ingest_dag antes das DAGs de detalhamento."
        )

    monkeypatch.setitem(preparar.__globals__, "ids_contratos_no_escopo", sem_raw)

    with pytest.raises(RuntimeError, match="contrato_ativo_ingest_dag"):
        preparar()


def test_responsaveis_sao_gravados_sem_alterar_o_payload(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A raw guarda o dado pessoal como veio; a proteção é a classificação."""
    ingerir = task(dagbag, "ingest_contracts")
    payload = [responsavel(1)]
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [payload, []]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    assert ingerir(["2289", "2290"]) == {"responsaveis": 1, "contratos_vazios": 1}
    assert cliente.listar_subrecurso.call_args_list == [
        call("2289", "responsaveis"),
        call("2290", "responsaveis"),
    ]
    escrita.assert_called_once_with(
        "contratos_gov",
        "contrato_responsavel",
        payload,
        primary_key=["id"],
        run_id=None,
    )
    gravado = escrita.call_args.args[2][0]
    assert gravado["data_inicio"] == "2024-01-02"
    assert gravado["data_fim"] is None
    assert gravado["situacao"] == "Ativo"
    assert gravado["instalacao_id"] == ""


def test_log_nao_expoe_dado_pessoal(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Log de task é lido por quem não tem nível para ver o dado pessoal."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.return_value = [responsavel(1)]
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", MagicMock())

    with caplog.at_level("DEBUG"):
        ingerir(["2289"])

    assert NOME_FICTICIO not in caplog.text


def test_contrato_sem_responsavel_nao_grava_e_segue(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Contrato sem responsável é comum (a amostra vai de 0 a 4): conta como vazio
    e não interrompe o bloco."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = [[], [responsavel(1, 2290)]]
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    assert ingerir(["2289", "2290"]) == {"responsaveis": 1, "contratos_vazios": 1}
    assert escrita.call_count == 1


def test_cada_contrato_tem_arquivo_proprio_no_object_storage(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """O backend append-only não sobrescreve outro contrato nem outro run."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.side_effect = lambda contrato_id, _: [
        responsavel(int(contrato_id), int(contrato_id))
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

    assert ingerir(["2289", "2290"])["responsaveis"] == 2
    run_id[0] = "segundo"
    assert ingerir(["2289", "2290"])["responsaveis"] == 2

    arquivos = sorted(tmp_path.rglob("*.parquet"))
    # O sufixo `--lote-<hash>` do backend sai da comparação: o que se fixa é
    # um arquivo por par run + contrato.
    assert len(arquivos) == 4
    assert {arquivo.stem.split("--lote-")[0] for arquivo in arquivos} == {
        "primeiro__contrato_2289",
        "primeiro__contrato_2290",
        "segundo__contrato_2289",
        "segundo__contrato_2290",
    }
    registro = pl.read_parquet(arquivos[0]).to_dicts()[0]
    assert registro["contrato_id"] in {2289, 2290}
    assert registro["dt_ingest"]


@pytest.mark.parametrize(
    ("payload", "motivo"),
    [
        ([{"id": 1, "contrato_id": "outro"}], "contrato_id divergente da URL"),
        ([{"contrato_id": "2289"}], "responsável sem id"),
        ([responsavel(1), responsavel(1)], "mesmo id duas vezes no lote"),
    ],
    ids=["contrato_id_divergente", "sem_id", "id_repetido"],
)
def test_lote_invalido_falha_antes_da_escrita(
    dagbag: DagBag,
    monkeypatch: pytest.MonkeyPatch,
    payload: list[dict],
    motivo: str,
) -> None:
    """id repetido quebra o ON CONFLICT, e contrato_id divergente apontaria o
    responsável, com os dados pessoais dele, para outro contrato."""
    ingerir = task(dagbag, "ingest_contracts")
    cliente = MagicMock()
    cliente.listar_subrecurso.return_value = payload
    escrita = MagicMock()
    monkeypatch.setitem(ingerir.__globals__, "ClienteContratosGov", lambda: cliente)
    monkeypatch.setitem(ingerir.__globals__, "write_raw", escrita)

    with pytest.raises(RuntimeError) as erro:
        ingerir(["2289"])

    escrita.assert_not_called()
    assert NOME_FICTICIO not in str(erro.value)


def test_validacao_conta_os_blocos(dagbag: DagBag) -> None:
    validar = task(dagbag, "validate")
    assert (
        validar(
            [
                {"responsaveis": 20, "contratos_vazios": 5},
                {"responsaveis": 1, "contratos_vazios": 24},
            ]
        )
        == 21
    )
