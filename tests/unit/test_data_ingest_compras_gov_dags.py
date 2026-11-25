import re
from pathlib import Path

import pytest
import yaml
from airflow.models import DagBag

pytestmark = pytest.mark.unit

DAGS_FOLDER = (
    Path(__file__).resolve().parents[2]
    / "airflow"
    / "dags"
    / "data_ingest"
    / "compras_gov"
)

TAG_FORMAT = re.compile(r"^[a-z_]+:[a-z0-9_]+$")


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


class TestComprasGovDagsIntegrity:
    def test_no_import_errors(self, dagbag: DagBag) -> None:
        assert dagbag.import_errors == {}, dagbag.import_errors

    def test_expected_number_of_dags_loaded(self, dagbag: DagBag) -> None:
        assert len(dagbag.dags) == 28

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
            if not any(t.startswith("sistema:") for t in dag.tags)
        ]
        assert not missing, f"DAGs sem tag sistema:: {missing}"

    def test_all_dags_have_owner(self, dagbag: DagBag) -> None:
        missing = [
            dag_id
            for dag_id, dag in dagbag.dags.items()
            if not dag.default_args.get("owner")
        ]
        assert not missing, f"DAGs sem owner em default_args: {missing}"


RAIZ = Path(__file__).resolve().parents[2]
BRONZES_PARQUET = sorted(
    p.stem for p in (RAIZ / "airflow/dags/dbt/compras_gov/models/bronze").glob("*.sql")
)


class TestAssetsDaRaw:
    @pytest.mark.parametrize("entidade", BRONZES_PARQUET)
    def test_ingestao_de_bronze_parquet_avisa_a_transformacao_no_fim(
        self, dagbag: DagBag, entidade: str
    ) -> None:
        """A duckdb_<orgao>_transform_dag é agendada pelo Asset da raw de cada
        Bronze em Parquet. A DAG de ingestão da entidade (a do catálogo) precisa
        emiti-lo numa task que só roda depois de todas as outras, senão a
        transformação leria uma raw com lotes faltando.
        """
        import landing_zone

        catalogo = yaml.safe_load(
            (RAIZ / "catalogo/sistemas/compras_gov.yml").read_text()
        )
        dag = dagbag.dags[catalogo["entidades"][entidade]["dag"]]
        asset = landing_zone.asset_raw("compras_gov", entidade)
        emissoras = [t for t in dag.tasks if asset in t.outlets]
        assert len(emissoras) == 1, [t.task_id for t in emissoras]
        anteriores = emissoras[0].get_flat_relative_ids(upstream=True)
        assert anteriores == {t.task_id for t in dag.tasks} - {emissoras[0].task_id}
