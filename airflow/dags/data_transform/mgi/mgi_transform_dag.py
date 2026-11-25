"""DAG de transformação do projeto dbt do MGI (ADR-0018).

Executa o projeto dbt do órgão — a Silver dos pacotes de sistema estruturante
que ele consome e a Gold que ele próprio materializa — com uma task do Airflow
por nó do grafo dbt, via Cosmos.

O grafo de dependências não é declarado aqui: o Cosmos o deriva dos `ref()` do
próprio projeto dbt. Um modelo novo entra na DAG sem nenhuma mudança neste
arquivo, e não há um segundo grafo para manter em sincronia.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior

ORGAO = "mgi"

# airflow/dags/data_transform/<orgao>/<arquivo>.py -> airflow/dags/dbt
DIR_DBT = Path(__file__).resolve().parents[2] / "dbt"

default_args = {
    "owner": ORGAO,
    "queue": ORGAO,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Profile compartilhado por todos os projetos dbt do monorepo (ADR-0017): lê
# host, usuário e senha do ambiente, para servir ao compose local, à homologação
# e à produção sem credencial versionada.
perfil = ProfileConfig(
    profile_name="gov_bricks",
    target_name=os.environ.get("DBT_TARGET", "dev"),
    profiles_yml_filepath=DIR_DBT / "profiles.yml",
)

mgi_transform_dag = DbtDag(
    dag_id=f"{ORGAO}_transform_dag",
    # O projeto do órgão importa os pacotes dos sistemas estruturantes que
    # consome (packages.yml); o Cosmos instala esses pacotes locais antes de
    # montar o grafo, senão o `ref()` entre pacotes não resolve.
    #
    # GOV_BRICKS_DBT_DIR é o que faz esses pacotes locais resolverem: o Cosmos
    # copia o projeto para um diretório temporário antes de rodar `dbt deps`, e
    # de lá o `../gov_bricks` do packages.yml apontaria para fora do monorepo.
    project_config=ProjectConfig(
        dbt_project_path=DIR_DBT / ORGAO,
        env_vars={"GOV_BRICKS_DBT_DIR": str(DIR_DBT)},
    ),
    profile_config=perfil,
    render_config=RenderConfig(
        # Testa cada modelo logo depois de materializá-lo, em vez de acumular os
        # testes no fim: assim um defeito na Silver interrompe o ramo afetado
        # antes de a Gold ser construída em cima de dado ruim.
        test_behavior=TestBehavior.AFTER_EACH,
        # Nós com tag `duckdb` leem Parquet com funções do DuckDB e falhariam no
        # PostgreSQL deste target; rodam na duckdb_mgi_transform_dag. Eles
        # continuam habilitados, e não `enabled=false`, para que um modelo daqui
        # possa dar ref() neles.
        exclude=["tag:duckdb"],
    ),
    # Depois da janela de ingestão (as DAGs do compras_gov rodam 01:00–02:00).
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Materializa Silver e Gold do projeto dbt do MGI, uma task por modelo "
        "(Cosmos, ADR-0018)."
    ),
    tags=[
        "orgao:mgi",
        "sistema:compras_gov",
        "camada:silver",
        "camada:gold",
    ],
)
