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

    def test_all_dags_have_owner(self, dagbag: DagBag) -> None:
        missing = [
            dag_id
            for dag_id, dag in dagbag.dags.items()
            if not dag.default_args.get("owner")
        ]
        assert not missing, f"DAGs sem owner em default_args: {missing}"
