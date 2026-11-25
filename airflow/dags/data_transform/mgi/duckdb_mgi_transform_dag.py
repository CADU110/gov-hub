"""DAG de transformação dos domínios em Parquet do projeto dbt do MGI (ADR-0018).

Executa os nós com tag `duckdb` — as Bronzes que leem os Parquets da zona raw e
as Silvers construídas sobre elas — no target `prod_duckdb`: o DuckDB lê os
Parquets do object storage e grava as tabelas no PostgreSQL anexado. O restante
do projeto roda na `mgi_transform_dag`, no PostgreSQL, que exclui esses nós.

É uma DAG à parte, e não um target diferente na DAG do projeto, porque os
sources relacionais e a Gold não declaram `database` e não resolvem no DuckDB
(ver profiles.yml).

Um domínio novo entra sem mudar este arquivo: basta a Bronze em
`dbt/<sistema>/models/bronze/<entidade>.sql` (via `gov_bricks.bronze_parquet`)
e a tag `duckdb` na pasta da Silver. O agendamento é derivado da mesma
convenção: cada Bronze em Parquet vira um Asset da raw (`landing_zone.asset_raw`),
emitido pela DAG de ingestão da entidade ao terminar. Um órgão novo copia este
arquivo trocando `ORGAO`, como na `mgi_transform_dag`.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow.sdk import AssetAny
from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior

from landing_zone import asset_raw

ORGAO = "mgi"

# airflow/dags/data_transform/<orgao>/<arquivo>.py -> airflow/dags/dbt
DIR_DBT = Path(__file__).resolve().parents[2] / "dbt"


def _pacotes_importados(dir_projeto: Path) -> list[str]:
    """Pacotes locais que o projeto do órgão importa no packages.yml."""
    pacotes = yaml.safe_load((dir_projeto / "packages.yml").read_text())["packages"]
    return [p["local"].rstrip("/").rsplit("/", 1)[-1] for p in pacotes if "local" in p]


def _bronzes_parquet(dir_dbt: Path, pacotes: list[str]) -> list[tuple[str, str]]:
    """Pares (sistema, entidade) das Bronzes em Parquet dos pacotes importados.

    Segue a convenção do `gov_bricks.bronze_parquet`: o pacote é o sistema e o
    arquivo do modelo, a entidade. A pasta `bronze/` só tem modelos que leem
    Parquet; a Bronze relacional é declarada como source.
    """
    return [
        (pacote, modelo.stem)
        for pacote in pacotes
        for modelo in sorted((dir_dbt / pacote / "models" / "bronze").glob("*.sql"))
    ]


BRONZES = _bronzes_parquet(DIR_DBT, _pacotes_importados(DIR_DBT / ORGAO))
SISTEMAS = sorted({sistema for sistema, _ in BRONZES})

default_args = {
    "owner": ORGAO,
    "queue": ORGAO,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

perfil = ProfileConfig(
    profile_name="gov_bricks",
    target_name="prod_duckdb",
    profiles_yml_filepath=DIR_DBT / "profiles.yml",
)

duckdb_mgi_transform_dag = DbtDag(
    dag_id=f"duckdb_{ORGAO}_transform_dag",
    project_config=ProjectConfig(
        dbt_project_path=DIR_DBT / ORGAO,
        env_vars={
            # Ver mgi_transform_dag: o Cosmos copia o projeto para um diretório
            # temporário, de onde o `..` do packages.yml sairia do monorepo.
            "GOV_BRICKS_DBT_DIR": str(DIR_DBT),
            # DuckDB em memória: cada task do Cosmos é um processo dbt próprio e
            # as tabelas vão para o PostgreSQL anexado, então nada precisa ficar
            # em arquivo. Um arquivo teria lock de escrita disputado por tasks
            # paralelas (os testes de uma Silver e os de outra, por exemplo).
            "DUCKDB_GOVHUB_PATH": ":memory:",
        },
    ),
    profile_config=perfil,
    render_config=RenderConfig(
        # Os testes dos nós selecionados entram por seleção indireta.
        select=["tag:duckdb"],
        test_behavior=TestBehavior.AFTER_EACH,
        # Testes que comparam Silver e Bronze têm dois pais. Sem isto, o Cosmos
        # os roda junto com os testes da Bronze, antes de a Silver ser
        # reconstruída, e eles comparariam a Bronze nova com a Silver anterior.
        should_detach_multiple_parents_tests=True,
    ),
    # Qualquer entidade que termine de ser ingerida dispara a reconstrução.
    # Esperar todas (AssetAll) deixaria todos os domínios parados por uma
    # ingestão que falhou. Uma execução disparada no meio da ingestão de outra
    # entidade lê os lotes parciais dela, mas a conclusão daquela ingestão
    # dispara outra execução, que completa a Bronze.
    schedule=AssetAny(*(asset_raw(s, e) for s, e in BRONZES)) if BRONZES else None,
    # Cada execução reconstrói as tabelas inteiras; duas ao mesmo tempo só
    # disputariam o mesmo destino.
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Materializa Bronze e Silver dos domínios que leem a raw em Parquet, com "
        "DuckDB gravando no PostgreSQL (Cosmos, ADR-0018)."
    ),
    tags=[
        f"orgao:{ORGAO}",
        *(f"sistema:{sistema}" for sistema in SISTEMAS),
        "camada:bronze",
        "camada:silver",
    ],
)
