"""
Comportamento da contrato_inativo_ingest_dag (issue #19).

As regras estruturais (dag_id, tags, owner, description) já são cobradas para
toda a pasta em test_data_ingest_contratos_gov_dags.py. Este arquivo cobre o que
é desta DAG: varrer as UGs da raw em blocos, uma escrita por UG, e a diferença
em relação aos ativos — UG sem contrato inativo é a regra, então execução sem
nenhum contrato só falha quando a varredura foi grande o bastante para isso ser
anomalia.
"""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call

import pytest
from airflow.models import DagBag
from landing_zone import RawIndisponivel

pytestmark = pytest.mark.unit

DAG_FILE = (
    Path(__file__).resolve().parents[2]
    / "airflow"
    / "dags"
    / "data_ingest"
    / "contratos_gov"
    / "contrato_inativo_ingest_dag.py"
)
DAG_ID = "contrato_inativo_ingest_dag"
SISTEMA = "contratos_gov"
ENTIDADE = "contrato_inativo"
BLOCK_SIZE = 25  # espelha a constante da DAG
MIN_UGS = 50  # espelha MIN_UGS_PARA_EXIGIR_CONTRATO
UG_A = "153173"
UG_B = "200999"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_FILE), include_examples=False)


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
    """Faz listar_contratos_inativos_ug responder conforme `por_ug`."""
    cliente = MagicMock()
    cliente.listar_contratos_inativos_ug.side_effect = lambda codigo: por_ug[codigo]
    monkeypatch.setitem(alvo.__globals__, "ClienteContratosGov", lambda: cliente)
    return cliente


def capturar_escritas(alvo: Any, monkeypatch: pytest.MonkeyPatch):
    """Substitui write_raw por um espião, para inspecionar as chamadas."""
    escrita = MagicMock()
    monkeypatch.setitem(alvo.__globals__, "write_raw", escrita)
    return escrita


def ugs_falsas(quantidade: int) -> list[str]:
    return [f"{codigo:06d}" for codigo in range(quantidade)]


def test_dag_carrega_sem_erro(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, dagbag.import_errors
    assert DAG_ID in dagbag.dags


def test_agenda_semanal_depois_dos_ativos(dagbag: DagBag) -> None:
    """Sábado 10:00: duas horas depois da varredura de ativos (sábado 08:00)."""
    assert dagbag.dags[DAG_ID].timetable.expression == "0 10 * * 6"


class TestBlocosDeUgs:
    def test_le_a_lista_da_entidade_unidade_contratante(self, dagbag, monkeypatch):
        blocos = task(dagbag, "get_ug_blocks")
        consulta = usar_ugs_da_raw(blocos, monkeypatch, [UG_A])

        blocos()

        consulta.assert_called_once_with(SISTEMA, "unidade_contratante", "codigo")

    def test_divide_a_lista_em_blocos_sem_perder_ug(self, dagbag, monkeypatch):
        blocos = task(dagbag, "get_ug_blocks")
        ugs = ugs_falsas(60)
        monkeypatch.delenv("INGEST_MAX_UGS", raising=False)
        usar_ugs_da_raw(blocos, monkeypatch, ugs)

        resultado = blocos()

        assert [len(bloco) for bloco in resultado] == [BLOCK_SIZE, BLOCK_SIZE, 10]
        assert [ug for bloco in resultado for ug in bloco] == ugs

    def test_respeita_o_teto_local(self, dagbag, monkeypatch):
        blocos = task(dagbag, "get_ug_blocks")
        monkeypatch.setenv("INGEST_MAX_UGS", "10")
        usar_ugs_da_raw(blocos, monkeypatch, ugs_falsas(100))

        assert sum(len(bloco) for bloco in blocos()) == 10

    def test_raw_ausente_diz_qual_dag_rodar_antes(self, dagbag, monkeypatch):
        blocos = task(dagbag, "get_ug_blocks")
        consulta = usar_ugs_da_raw(blocos, monkeypatch, [])
        consulta.side_effect = RawIndisponivel("raw_unidade_contratante")

        with pytest.raises(RuntimeError, match="unidade_contratante_ingest_dag"):
            blocos()

    def test_raw_vazia_falha_antes_de_varrer(self, dagbag, monkeypatch):
        blocos = task(dagbag, "get_ug_blocks")
        usar_ugs_da_raw(blocos, monkeypatch, [])

        with pytest.raises(RuntimeError, match="unidade_contratante"):
            blocos()


class TestIngestaoPorUg:
    def test_grava_um_lote_por_ug_com_chave_primaria(self, dagbag, monkeypatch):
        ingestao = task(dagbag, "ingest_ugs")
        usar_cliente(
            ingestao, monkeypatch, {UG_A: [{"id": 1}, {"id": 2}], UG_B: [{"id": 3}]}
        )
        escrita = capturar_escritas(ingestao, monkeypatch)

        resultado = ingestao([UG_A, UG_B])

        assert escrita.call_args_list == [
            call(SISTEMA, ENTIDADE, [{"id": 1}, {"id": 2}], primary_key=["id"]),
            call(SISTEMA, ENTIDADE, [{"id": 3}], primary_key=["id"]),
        ]
        assert resultado == {"contratos": 3, "ugs_vazias": []}

    def test_ug_sem_contrato_inativo_nao_grava(self, dagbag, monkeypatch):
        ingestao = task(dagbag, "ingest_ugs")
        usar_cliente(ingestao, monkeypatch, {UG_A: [], UG_B: [{"id": 7}]})
        escrita = capturar_escritas(ingestao, monkeypatch)

        resultado = ingestao([UG_A, UG_B])

        assert escrita.call_args_list == [
            call(SISTEMA, ENTIDADE, [{"id": 7}], primary_key=["id"])
        ]
        assert resultado["ugs_vazias"] == [UG_A]

    def test_usa_o_endpoint_de_inativos(self, dagbag, monkeypatch):
        """Trocar por listar_contratos_ug gravaria ativos na raw de inativos."""
        ingestao = task(dagbag, "ingest_ugs")
        cliente = usar_cliente(ingestao, monkeypatch, {UG_A: []})
        capturar_escritas(ingestao, monkeypatch)

        ingestao([UG_A])

        cliente.listar_contratos_inativos_ug.assert_called_once_with(UG_A)
        cliente.listar_contratos_ug.assert_not_called()

    def test_erro_da_api_interrompe_o_bloco(self, dagbag, monkeypatch):
        """Erro de rede não vira lote vazio: a task falha e o Airflow refaz."""
        ingestao = task(dagbag, "ingest_ugs")
        cliente = usar_cliente(ingestao, monkeypatch, {})
        cliente.listar_contratos_inativos_ug.side_effect = ConnectionError("timeout")
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

    def test_varredura_pequena_sem_contrato_nao_falha(self, dagbag, caplog):
        """As 10 UGs do INGEST_MAX_UGS local voltam todas vazias com frequência:
        a maioria das UGs não tem contrato inativo."""
        validacao = task(dagbag, "validate")

        with caplog.at_level("WARNING"):
            total = validacao([{"contratos": 0, "ugs_vazias": ugs_falsas(10)}])

        assert total == 0
        assert any(r.levelname == "WARNING" for r in caplog.records)

    def test_varredura_grande_sem_contrato_falha(self, dagbag):
        validacao = task(dagbag, "validate")

        with pytest.raises(RuntimeError, match="nenhum contrato inativo"):
            validacao([{"contratos": 0, "ugs_vazias": ugs_falsas(MIN_UGS)}])


class TestUgsQueZeraram:
    def test_le_as_ugs_da_propria_raw_de_contrato_inativo(self, dagbag, monkeypatch):
        antes = task(dagbag, "get_ugs_com_contrato")
        consulta = usar_ugs_da_raw(antes, monkeypatch, [UG_A])

        assert antes() == [UG_A]
        consulta.assert_called_once_with(SISTEMA, ENTIDADE, "unidade_gestora_codigo")

    def test_primeira_execucao_sem_raw_nao_falha(self, dagbag, monkeypatch):
        antes = task(dagbag, "get_ugs_com_contrato")
        consulta = usar_ugs_da_raw(antes, monkeypatch, [])
        consulta.side_effect = RawIndisponivel("raw_contrato_inativo")

        assert antes() == []

    def test_roda_antes_da_varredura(self, dagbag):
        dag = dagbag.dags[DAG_ID]

        assert "ingest_ugs" in dag.get_task("get_ugs_com_contrato").downstream_task_ids

    def test_ug_que_tinha_contrato_e_zerou_gera_alerta(self, dagbag, caplog):
        validacao = task(dagbag, "validate")

        with caplog.at_level("WARNING"):
            validacao([{"contratos": 5, "ugs_vazias": [UG_A]}], [UG_A, UG_B])

        alertas = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(alertas) == 1
        assert UG_A in alertas[0].getMessage()

    def test_ug_que_sempre_foi_vazia_nao_gera_alerta(self, dagbag, caplog):
        validacao = task(dagbag, "validate")

        with caplog.at_level("WARNING"):
            validacao([{"contratos": 5, "ugs_vazias": [UG_A]}], [UG_B])

        assert not [r for r in caplog.records if r.levelname == "WARNING"]
