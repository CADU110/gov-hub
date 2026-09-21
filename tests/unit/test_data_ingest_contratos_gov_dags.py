"""
Integridade das DAGs de ingestão do Contratos.gov.br.

Molde do test_data_ingest_compras_gov_dags.py, com uma diferença: a contagem
esperada vem do disco, não de um número fixo. A pasta nasce vazia na fundação
(issue #15) e cada issue filha (#16 a #33) acrescenta uma DAG — um número fixo
obrigaria a editar este arquivo 18 vezes sem ganhar nada, já que o que ele
protege são as regras estruturais, não a quantidade.
"""

import re
from pathlib import Path

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
TAGS_OBRIGATORIAS = {"sistema:contratos_gov", "dominio:contratacoes"}


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

    def test_all_dags_have_owner(self, dagbag: DagBag) -> None:
        missing = [
            dag_id
            for dag_id, dag in dagbag.dags.items()
            if not dag.default_args.get("owner")
        ]
        assert not missing, f"DAGs sem owner em default_args: {missing}"


def test_empty_response_fails_before_write(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    fetch_and_store = (
        dagbag.dags["orgao_contratante_ingest_dag"]
        .get_task("fetch_and_store")
        .python_callable
    )

    class ClienteVazio:
        def listar_orgaos(self) -> list[dict]:
            return []

    def escrita_proibida(*args: object, **kwargs: object) -> None:
        pytest.fail("write_raw não pode ser chamado para uma resposta vazia")

    monkeypatch.setitem(fetch_and_store.__globals__, "ClienteContratosGov", ClienteVazio)
    monkeypatch.setitem(fetch_and_store.__globals__, "write_raw", escrita_proibida)

    with pytest.raises(RuntimeError, match="Resposta vazia"):
        fetch_and_store()


def test_valid_response_is_written_unchanged_with_primary_key(
    dagbag: DagBag, monkeypatch: pytest.MonkeyPatch
) -> None:
    fetch_and_store = (
        dagbag.dags["orgao_contratante_ingest_dag"]
        .get_task("fetch_and_store")
        .python_callable
    )
    payload = [{"codigo": "02000"}, {"codigo": "46000"}]
    chamadas: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class ClienteFake:
        def listar_orgaos(self) -> list[dict[str, str]]:
            return payload

    def registrar_escrita(*args: object, **kwargs: object) -> None:
        chamadas.append((args, kwargs))

    monkeypatch.setitem(fetch_and_store.__globals__, "ClienteContratosGov", ClienteFake)
    monkeypatch.setitem(fetch_and_store.__globals__, "write_raw", registrar_escrita)

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
        ([{"codigo": "2000"}], "codigo inválido"),
        ([{"codigo": 2000}], "codigo inválido"),
        ([{"codigo": "02000"}, {"codigo": "02000"}], "Códigos duplicados"),
    ],
)
def test_invalid_or_duplicate_codes_fail_before_write(
    dagbag: DagBag,
    monkeypatch: pytest.MonkeyPatch,
    payload: list[dict[str, object]],
    mensagem: str,
) -> None:
    fetch_and_store = (
        dagbag.dags["orgao_contratante_ingest_dag"]
        .get_task("fetch_and_store")
        .python_callable
    )

    class ClienteFake:
        def listar_orgaos(self) -> list[dict[str, object]]:
            return payload

    def escrita_proibida(*args: object, **kwargs: object) -> None:
        pytest.fail("write_raw não pode ser chamado para um lote inválido")

    monkeypatch.setitem(fetch_and_store.__globals__, "ClienteContratosGov", ClienteFake)
    monkeypatch.setitem(fetch_and_store.__globals__, "write_raw", escrita_proibida)

    with pytest.raises(RuntimeError, match=mensagem):
        fetch_and_store()
