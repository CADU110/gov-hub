"""
Integridade das DAGs de ingestão do Contratos.gov.br.

Molde do test_data_ingest_compras_gov_dags.py, sempre que uma dag diferente entrar teremos que modificar esse teste.
"""

import re
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call

import pytest
from airflow.models import DagBag

pytestmark = pytest.mark.unit

DAGS_FOLDER = (
    Path(__file__).resolve().parents[2]
    / "airflow"
    / "dags"
    / "data_ingest"
    / "contratos_gov"
)

TAG_FORMAT = re.compile(r"^[a-z_]+:[a-z0-9_]+$")


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


class TestContratosGovDagsIntegrity:
    def test_no_import_errors(self, dagbag: DagBag) -> None:
        assert dagbag.import_errors == {}, dagbag.import_errors

    def test_every_dag_file_loaded(self, dagbag: DagBag) -> None:
        assert len(dagbag.dags) == len(list(DAGS_FOLDER.glob("*.py")))

    def test_dag_ids_match_filenames(self, dagbag: DagBag) -> None:
        py_files = {p.stem for p in DAGS_FOLDER.glob("*.py")}
        loaded_ids = set(dagbag.dags.keys())
        assert loaded_ids == py_files

    def test_all_dag_ids_end_with_ingest_dag(self, dagbag: DagBag) -> None:
        for dag_id in dagbag.dags:
            assert dag_id.endswith("_ingest_dag"), dag_id

    def test_all_dags_have_description(self, dagbag: DagBag) -> None:
        missing = [dag_id for dag_id, dag in dagbag.dags.items() if not dag.description]
        assert not missing, f"DAGs sem description: {missing}"

    def test_all_tags_follow_adr_0008_format(self, dagbag: DagBag) -> None:
        violations = {}
        for dag_id, dag in dagbag.dags.items():
            bad_tags = [t for t in dag.tags if not TAG_FORMAT.match(t)]
            if bad_tags:
                violations[dag_id] = bad_tags
        assert not violations, f"Tags fora do formato dimensao:valor: {violations}"

    def test_all_dags_have_sistema_tag(self, dagbag: DagBag) -> None:
        missing = [
            dag_id
            for dag_id, dag in dagbag.dags.items()
            if "sistema:contratos_gov" not in dag.tags
        ]
        assert not missing, f"DAGs sem a tag sistema:contratos_gov: {missing}"

    def test_all_dags_have_dominio_tag(self, dagbag: DagBag) -> None:
        missing = [
            dag_id
            for dag_id, dag in dagbag.dags.items()
            if not any(t.startswith("dominio:") for t in dag.tags)
        ]
        assert not missing, f"DAGs sem tag dominio:: {missing}"

    def test_all_dags_have_owner(self, dagbag: DagBag) -> None:
        missing = [
            dag_id
            for dag_id, dag in dagbag.dags.items()
            if not dag.default_args.get("owner")
        ]
        assert not missing, f"DAGs sem owner em default_args: {missing}"


# --- contrato_ativo (issue #18) ---------------------------------------------
#
# A DAG varre uma UG por chamada: a API não pagina nem filtra por período, e a
# lista de UGs vem da raw que a unidade_contratante_ingest_dag (issue #16)
# grava. Daí o que os testes cobram: particionar a lista em blocos em vez de
# expandir sobre as 3.781 UGs (batching.py), falhar cedo quando a dependência
# não foi ingerida, e não deixar uma UG vazia derrubar o bloco inteiro.

DAG_ID = "contrato_ativo_ingest_dag"
SISTEMA = "contratos_gov"
ENTIDADE = "contrato_ativo"
BLOCK_SIZE = 25  # espelha a constante da DAG
UG_A = "153173"
UG_B = "200999"


def task(dagbag: DagBag, task_id: str) -> Any:
    """Função Python por trás de uma task do TaskFlow, para chamar direto.

    O retorno é `Any` porque os helpers abaixo mexem no `__globals__` da função,
    atributo que `Callable` não declara e que faria o `ty` reprovar.
    """
    return dagbag.dags[DAG_ID].get_task(task_id).python_callable


def usar_ugs_da_raw(alvo: Any, monkeypatch: pytest.MonkeyPatch, ugs: list[str]):
    """Faz distinct_raw_values devolver `ugs` e registra como foi chamada."""
    consulta = MagicMock(return_value=ugs)
    monkeypatch.setitem(alvo.__globals__, "distinct_raw_values", consulta)
    return consulta


def usar_cliente(alvo: Any, monkeypatch: pytest.MonkeyPatch, por_ug: dict):
    """Faz listar_contratos_ug responder conforme `por_ug`, uma entrada por UG."""
    cliente = MagicMock()
    cliente.listar_contratos_ug.side_effect = lambda codigo: por_ug[codigo]
    monkeypatch.setitem(alvo.__globals__, "ClienteContratosGov", lambda: cliente)
    return cliente


def capturar_escritas(alvo: Any, monkeypatch: pytest.MonkeyPatch):
    """Substitui write_raw por um espião, para inspecionar as chamadas."""
    escrita = MagicMock()
    monkeypatch.setitem(alvo.__globals__, "write_raw", escrita)
    return escrita


def ugs_falsas(quantidade: int) -> list[str]:
    return [f"{codigo:06d}" for codigo in range(quantidade)]


class TestBlocosDeUgs:
    def test_le_a_lista_da_entidade_unidade_contratante(self, dagbag, monkeypatch):
        blocos = task(dagbag, "get_ug_blocks")
        consulta = usar_ugs_da_raw(blocos, monkeypatch, [UG_A])

        blocos()

        consulta.assert_called_once_with(SISTEMA, "unidade_contratante", "codigo")

    def test_divide_a_lista_em_blocos(self, dagbag, monkeypatch):
        blocos = task(dagbag, "get_ug_blocks")
        monkeypatch.delenv("INGEST_MAX_UGS", raising=False)
        usar_ugs_da_raw(blocos, monkeypatch, ugs_falsas(60))

        assert [len(bloco) for bloco in blocos()] == [BLOCK_SIZE, BLOCK_SIZE, 10]

    def test_nao_perde_nem_reordena_ug(self, dagbag, monkeypatch):
        blocos = task(dagbag, "get_ug_blocks")
        ugs = ugs_falsas(60)
        monkeypatch.delenv("INGEST_MAX_UGS", raising=False)
        usar_ugs_da_raw(blocos, monkeypatch, ugs)

        assert [ug for bloco in blocos() for ug in bloco] == ugs

    def test_respeita_o_teto_local(self, dagbag, monkeypatch):
        """INGEST_MAX_UGS existe só no local.env: a varredura completa não roda
        em máquina de desenvolvedor."""
        blocos = task(dagbag, "get_ug_blocks")
        monkeypatch.setenv("INGEST_MAX_UGS", "10")
        usar_ugs_da_raw(blocos, monkeypatch, ugs_falsas(100))

        assert sum(len(bloco) for bloco in blocos()) == 10

    def test_raw_ausente_diz_qual_dag_rodar_antes(self, dagbag, monkeypatch):
        blocos = task(dagbag, "get_ug_blocks")
        consulta = usar_ugs_da_raw(blocos, monkeypatch, [])
        consulta.side_effect = FileNotFoundError("raw_unidade_contratante")

        with pytest.raises(RuntimeError) as erro:
            blocos()

        assert "unidade_contratante_ingest_dag" in str(erro.value)

    def test_raw_vazia_falha_antes_de_varrer(self, dagbag, monkeypatch):
        """Tabela existente e vazia é anomalia: sem UG não há o que varrer."""
        blocos = task(dagbag, "get_ug_blocks")
        usar_ugs_da_raw(blocos, monkeypatch, [])

        with pytest.raises(RuntimeError) as erro:
            blocos()

        assert "unidade_contratante" in str(erro.value)


class TestIngestaoPorUg:
    def test_grava_um_lote_por_ug(self, dagbag, monkeypatch):
        """Uma escrita por UG, não uma por bloco: falha no meio do bloco não
        descarta o que já veio, e a contagem por UG sobra no log."""
        ingestao = task(dagbag, "ingest_ugs")
        usar_cliente(
            ingestao, monkeypatch, {UG_A: [{"id": 1}, {"id": 2}], UG_B: [{"id": 3}]}
        )
        escrita = capturar_escritas(ingestao, monkeypatch)

        ingestao([UG_A, UG_B])

        assert escrita.call_args_list == [
            call(SISTEMA, ENTIDADE, [{"id": 1}, {"id": 2}], primary_key=["id"]),
            call(SISTEMA, ENTIDADE, [{"id": 3}], primary_key=["id"]),
        ]

    def test_conta_os_contratos_do_bloco(self, dagbag, monkeypatch):
        ingestao = task(dagbag, "ingest_ugs")
        usar_cliente(
            ingestao, monkeypatch, {UG_A: [{"id": 1}, {"id": 2}], UG_B: [{"id": 3}]}
        )
        capturar_escritas(ingestao, monkeypatch)

        assert ingestao([UG_A, UG_B])["contratos"] == 3

    def test_ug_sem_contratos_nao_grava(self, dagbag, monkeypatch):
        """UG inexistente e UG sem contrato respondem igual (200 []): registra
        a UG vazia e segue para a próxima."""
        ingestao = task(dagbag, "ingest_ugs")
        usar_cliente(ingestao, monkeypatch, {UG_A: [], UG_B: [{"id": 7}]})
        escrita = capturar_escritas(ingestao, monkeypatch)

        resultado = ingestao([UG_A, UG_B])

        assert escrita.call_args_list == [
            call(SISTEMA, ENTIDADE, [{"id": 7}], primary_key=["id"])
        ]
        assert resultado["ugs_vazias"] == [UG_A]

    def test_erro_da_api_interrompe_o_bloco(self, dagbag, monkeypatch):
        """Erro de rede não vira lote vazio: a task falha e o Airflow refaz o
        bloco (retries em default_args)."""
        ingestao = task(dagbag, "ingest_ugs")
        cliente = usar_cliente(ingestao, monkeypatch, {UG_A: [{"id": 1}]})
        cliente.listar_contratos_ug.side_effect = ConnectionError("timeout")
        capturar_escritas(ingestao, monkeypatch)

        with pytest.raises(ConnectionError):
            ingestao([UG_A])


class TestValidacaoFinal:
    def test_soma_os_totais_dos_blocos(self, dagbag):
        validacao = task(dagbag, "validate")

        total = validacao(
            [
                {"contratos": 10, "ugs_vazias": []},
                {"contratos": 5, "ugs_vazias": [UG_B]},
            ]
        )

        assert total == 15

    def test_execucao_sem_nenhum_contrato_falha(self, dagbag):
        """Todas as UGs vazias na mesma execução é sinal de fonte fora do ar,
        não de realidade."""
        validacao = task(dagbag, "validate")

        with pytest.raises(RuntimeError) as erro:
            validacao([{"contratos": 0, "ugs_vazias": [UG_A, UG_B]}])

        assert "nenhum contrato" in str(erro.value)
