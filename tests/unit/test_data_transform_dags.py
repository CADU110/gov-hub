"""Testes das DAGs de transformação (ADR-0018).

Carregar a DAG aqui não é formalidade: o Cosmos monta o grafo rodando `dbt ls`
no projeto durante o import, então este teste é o que garante que o projeto dbt
e os pacotes locais que ele importa continuam resolvendo — falha que, sem isto,
só apareceria no DAG processor em execução.
"""

import re
from pathlib import Path

import pytest
import yaml
from airflow.models import DagBag

pytestmark = pytest.mark.unit

DAGS_FOLDER = Path(__file__).resolve().parents[2] / "airflow" / "dags" / "data_transform"

TAG_FORMAT = re.compile(r"^[a-z_]+:[a-z0-9_]+$")


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


class TestDagsTransformacaoIntegridade:
    def test_sem_erro_de_import(self, dagbag: DagBag) -> None:
        assert dagbag.import_errors == {}, dagbag.import_errors

    def test_ha_ao_menos_uma_dag(self, dagbag: DagBag) -> None:
        assert dagbag.dags, "nenhuma DAG de transformação carregada"

    def test_dag_ids_correspondem_aos_nomes_dos_arquivos(self, dagbag: DagBag) -> None:
        arquivos = {p.stem for p in DAGS_FOLDER.rglob("*_transform_dag.py")}
        assert set(dagbag.dags) == arquivos

    def test_dag_ids_terminam_em_transform_dag(self, dagbag: DagBag) -> None:
        for dag_id in dagbag.dags:
            assert dag_id.endswith("_transform_dag"), dag_id

    def test_arquivo_fica_em_pasta_de_orgao(self, dagbag: DagBag) -> None:
        """ADR-0018: transformação é sempre específica de um órgão."""
        for caminho in DAGS_FOLDER.rglob("*_transform_dag.py"):
            orgao = caminho.parent.name
            assert (
                caminho.parent.parent == DAGS_FOLDER
            ), f"{caminho} deve estar em data_transform/<orgao>/"
            assert caminho.stem.endswith(
                f"{orgao}_transform_dag"
            ), f"{caminho.name} deve conter o órgão '{orgao}' no nome"

    def test_todas_tem_description(self, dagbag: DagBag) -> None:
        sem = [i for i, d in dagbag.dags.items() if not d.description]
        assert not sem, f"DAGs sem description: {sem}"

    def test_tags_seguem_o_formato_do_adr_0008(self, dagbag: DagBag) -> None:
        violacoes = {
            i: [t for t in d.tags if not TAG_FORMAT.match(t)]
            for i, d in dagbag.dags.items()
            if any(not TAG_FORMAT.match(t) for t in d.tags)
        }
        assert not violacoes, f"Tags fora do formato dimensao:valor: {violacoes}"

    def test_todas_tem_tag_de_orgao(self, dagbag: DagBag) -> None:
        sem = [
            i
            for i, d in dagbag.dags.items()
            if not any(t.startswith("orgao:") for t in d.tags)
        ]
        assert not sem, f"DAGs de transformação sem tag orgao:: {sem}"

    def test_todas_tem_owner(self, dagbag: DagBag) -> None:
        sem = [i for i, d in dagbag.dags.items() if not d.default_args.get("owner")]
        assert not sem, f"DAGs sem owner em default_args: {sem}"


class TestGrafoDbtRenderizado:
    def test_mgi_expande_em_uma_task_por_modelo(self, dagbag: DagBag) -> None:
        """O ganho do Cosmos sobre um `dbt build` em BashOperator (ADR-0018).

        Se o grafo colapsar em uma única task, a decisão do ADR deixou de valer
        na prática — e o retry volta a reprocessar o projeto inteiro.
        """
        dag = dagbag.dags["mgi_transform_dag"]
        tasks = {t.task_id for t in dag.tasks}

        # A Silver do pacote compras_gov e a Gold do próprio MGI, cada modelo
        # como sua própria task.
        for modelo in ("contratos", "uasg", "orgao", "contratos_por_orgao"):
            assert any(
                t.startswith(modelo) for t in tasks
            ), f"modelo '{modelo}' não virou task: {sorted(tasks)}"
        assert len(tasks) > 1


def _no(task) -> dict:
    """O nó dbt que o Cosmos associou à task."""
    return task.extra_context["dbt_node_config"]


def _bronzes_parquet(orgao: str) -> list[tuple[str, str]]:
    """(sistema, entidade) de cada Bronze em Parquet dos pacotes do órgão."""
    dbt = DAGS_FOLDER.parent / "dbt"
    pacotes = [
        p["local"].rsplit("/", 1)[-1]
        for p in yaml.safe_load((dbt / orgao / "packages.yml").read_text())["packages"]
    ]
    return [
        (pacote, modelo.stem)
        for pacote in pacotes
        for modelo in sorted((dbt / pacote / "models/bronze").glob("*.sql"))
    ]


class TestSeparacaoPostgresDuckdb:
    """Cada órgão com nós `duckdb` tem as duas DAGs, sem nó em comum."""

    @pytest.fixture(params=["mgi"])
    def orgao(self, request) -> str:
        return request.param

    def test_nos_duckdb_ficam_fora_da_dag_do_projeto(self, dagbag, orgao) -> None:
        """Bronze em Parquet usa `read_parquet`, que não existe no PostgreSQL.

        Se a tag `duckdb` parar de excluir esses nós (ou os testes deles), a
        DAG do projeto inteiro volta a falhar todo dia.
        """
        duckdb_ids = {
            _no(t)["unique_id"]
            for t in dagbag.dags[f"duckdb_{orgao}_transform_dag"].tasks
            if _no(t)["resource_type"] == "model"
        }
        assert duckdb_ids
        for task in dagbag.dags[f"{orgao}_transform_dag"].tasks:
            no = _no(task)
            assert "duckdb" not in no["tags"], task.task_id
            assert not duckdb_ids & {no["unique_id"], *no["depends_on"]}, task.task_id

    def test_dag_duckdb_executa_toda_bronze_parquet(self, dagbag, orgao) -> None:
        dag = dagbag.dags[f"duckdb_{orgao}_transform_dag"]
        modelos = {_no(t)["unique_id"] for t in dag.tasks}
        for sistema, entidade in _bronzes_parquet(orgao):
            assert f"model.{sistema}.{entidade}" in modelos
        for task in dag.tasks:
            if _no(task)["resource_type"] == "model":
                assert "duckdb" in _no(task)["tags"], task.task_id

    def test_testes_com_dois_pais_rodam_depois_de_todos(self, dagbag, orgao) -> None:
        """Os que comparam Silver e Bronze: rodar junto com o teste da Bronze
        compararia a Bronze nova com a Silver da execução anterior."""
        dag = dagbag.dags[f"duckdb_{orgao}_transform_dag"]
        run = {
            _no(t)["unique_id"]: t.task_id for t in dag.tasks if t.task_id.endswith("run")
        }
        for task in dag.tasks:
            pais = [p for p in _no(task)["depends_on"] if p.startswith("model.")]
            if _no(task)["resource_type"] == "test" and len(pais) > 1:
                assert task.upstream_task_ids >= {run[p] for p in pais}, task.task_id

    def test_dag_duckdb_roda_em_memoria(self, dagbag, orgao) -> None:
        for task in dagbag.dags[f"duckdb_{orgao}_transform_dag"].tasks:
            assert task.profile_config.target_name == "prod_duckdb"
            assert task.env["DUCKDB_GOVHUB_PATH"] == ":memory:"

    def test_dag_duckdb_e_agendada_pelos_assets_da_raw(self, dagbag, orgao) -> None:
        import landing_zone

        dag = dagbag.dags[f"duckdb_{orgao}_transform_dag"]
        assert list(dag.timetable.asset_condition.objects) == [
            landing_zone.asset_raw(s, e) for s, e in _bronzes_parquet(orgao)
        ]
        assert type(dag.timetable.asset_condition).__name__ == "AssetAny"
        assert dag.max_active_runs == 1
