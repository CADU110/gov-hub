"""
Integridade das DAGs de ingestão do Contratos.gov.br.

Molde do test_data_ingest_compras_gov_dags.py, com uma diferença: a contagem
esperada vem do disco, não de um número fixo. A pasta nasce vazia na fundação
(issue #15) e cada issue filha (#16 a #33) acrescenta uma DAG — um número fixo
obrigaria a editar este arquivo 18 vezes sem ganhar nada, já que o que ele
protege são as regras estruturais, não a quantidade.
"""

import re
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call

import pytest
from airflow.models import DagBag
from landing_zone import RawIndisponivel

pytestmark = pytest.mark.unit

DAGS_FOLDER = (
    Path(__file__).resolve().parents[2]
    / "airflow"
    / "dags"
    / "data_ingest"
    / "contratos_gov"
)

TAG_FORMAT = re.compile(r"^[a-z_]+:[a-z0-9_]+$")
TAGS_OBRIGATORIAS = {"sistema:contratos_gov"}


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

    def test_all_dags_have_required_tags(self, dagbag: DagBag) -> None:
        missing = {
            dag_id: sorted(TAGS_OBRIGATORIAS - set(dag.tags))
            for dag_id, dag in dagbag.dags.items()
            if not TAGS_OBRIGATORIAS.issubset(dag.tags)
        }
        assert not missing, f"DAGs sem tags obrigatórias: {missing}"

    def test_all_dags_have_dominio_tag(self, dagbag: DagBag) -> None:
        missing = [
            dag_id
            for dag_id, dag in dagbag.dags.items()
            if not any(t.startswith("dominio:") for t in dag.tags)
        ]
        assert not missing, f"DAGs sem tag dominio: {missing}"

    def test_all_dags_have_owner(self, dagbag: DagBag) -> None:
        missing = [
            dag_id
            for dag_id, dag in dagbag.dags.items()
            if not dag.default_args.get("owner")
        ]
        assert not missing, f"DAGs sem owner em default_args: {missing}"

    def test_contrato_empenho_tem_metadados_do_dominio(self, dagbag: DagBag) -> None:
        dag = dagbag.dags["contrato_empenho_ingest_dag"]
        assert {"sistema:contratos_gov", "dominio:contratacoes"}.issubset(dag.tags)
        assert dag.default_args["owner"] == "mgi"
        assert dag.description

    def test_unidade_contratante_tem_dominio_organizacional(self, dagbag: DagBag) -> None:
        dag = dagbag.dags["unidade_contratante_ingest_dag"]
        assert "dominio:organizacional" in dag.tags


# --- orgao_contratante (issue #17) ------------------------------------------


def _preparar_fetch_and_store(
    dagbag: DagBag,
    monkeypatch: pytest.MonkeyPatch,
    payload: list[dict[str, object]],
    escrita: Callable[..., None],
) -> Callable[[], dict[str, int]]:
    fetch_and_store = (
        dagbag.dags["orgao_contratante_ingest_dag"]
        .get_task("fetch_and_store")
        .python_callable
    )

    class ClienteFake:
        def listar_orgaos(self) -> list[dict[str, object]]:
            return payload

    monkeypatch.setitem(fetch_and_store.__globals__, "ClienteContratosGov", ClienteFake)
    monkeypatch.setitem(fetch_and_store.__globals__, "write_raw", escrita)
    return fetch_and_store


def test_valid_response_is_written_unchanged_with_primary_key(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload: list[dict[str, object]] = [{"codigo": "02000"}, {"codigo": "46000"}]
    chamadas: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def registrar_escrita(*args: object, **kwargs: object) -> None:
        chamadas.append((args, kwargs))

    fetch_and_store = _preparar_fetch_and_store(
        dagbag, monkeypatch, payload, registrar_escrita
    )

    assert fetch_and_store() == {"ingeridos": 2}
    assert chamadas == [
        (
            ("contratos_gov", "orgao_contratante", payload),
            {"primary_key": ["codigo"]},
        )
    ]


@pytest.mark.parametrize(
    ("payload", "mensagem"),
    [
        ([], "Resposta vazia"),
        ([{}], "codigo inválido"),
        ([{"codigo": "2000"}], "codigo inválido"),
        ([{"codigo": "123456"}], "codigo inválido"),
        ([{"codigo": 2000}], "codigo inválido"),
        ([{"codigo": "02000"}, {"codigo": "02000"}], "Códigos duplicados"),
    ],
    ids=["vazia", "sem_codigo", "quatro_digitos", "seis_digitos", "inteiro", "duplicado"],
)
def test_invalid_response_fails_before_write(
    dagbag: DagBag,
    monkeypatch: pytest.MonkeyPatch,
    payload: list[dict[str, object]],
    mensagem: str,
) -> None:
    def escrita_proibida(*args: object, **kwargs: object) -> None:
        pytest.fail("write_raw não pode ser chamado para um lote inválido")

    fetch_and_store = _preparar_fetch_and_store(
        dagbag, monkeypatch, payload, escrita_proibida
    )

    with pytest.raises(RuntimeError, match=mensagem):
        fetch_and_store()


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


class TestUgsQueZeraram:
    """UG que já tinha contrato na raw e voltou vazia vira alerta, não erro."""

    def test_le_as_ugs_da_propria_raw_de_contrato_ativo(self, dagbag, monkeypatch):
        antes = task(dagbag, "get_ugs_com_contrato")
        consulta = usar_ugs_da_raw(antes, monkeypatch, [UG_A])

        assert antes() == [UG_A]
        consulta.assert_called_once_with(SISTEMA, ENTIDADE, "unidade_gestora_codigo")

    def test_primeira_execucao_sem_raw_nao_falha(self, dagbag, monkeypatch):
        antes = task(dagbag, "get_ugs_com_contrato")
        consulta = usar_ugs_da_raw(antes, monkeypatch, [])
        consulta.side_effect = RawIndisponivel("raw_contrato_ativo")

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
