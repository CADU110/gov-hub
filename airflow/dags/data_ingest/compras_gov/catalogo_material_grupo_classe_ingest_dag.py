import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from batching import page_starts
from cliente_compras_gov import ClienteComprasGov
from landing_zone import write_raw

SISTEMA = "compras_gov"
PAGE_SIZE = 500
BLOCK_SIZE = 15

GRUPO_ENDPOINT = "/modulo-material/1_consultarGrupoMaterial"
GRUPO_PARAMS = {"statusGrupo": "true"}
CLASSE_ENDPOINT = "/modulo-material/2_consultarClasseMaterial"
CLASSE_PARAMS = {"statusClasse": "true"}

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="catalogo_material_grupo_classe_ingest_dag",
    schedule="0 2 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere grupos e classes de materiais do catálogo do Compras.gov.br para compras_gov.raw_grupo_material e "
        "raw_classe_material."
    ),
    tags=["sistema:compras_gov", "dominio:material", "orgao:mgi"],
)
def catalogo_material_grupo_classe_dag() -> None:
    @task
    def get_page_starts(endpoint: str, query_params: dict) -> list[int]:
        api = ClienteComprasGov()
        _, resp = api.request(
            "GET",
            endpoint,
            params={**query_params, "pagina": 1, "tamanhoPagina": PAGE_SIZE},
        )
        if not isinstance(resp, dict):
            raise RuntimeError(f"A API não retornou resposta válida: {endpoint}.")
        total = resp.get("totalPaginas", 1)
        logging.info("[%s] Total de páginas: %s", endpoint, total)
        return page_starts(total, BLOCK_SIZE)

    @task(max_active_tis_per_dag=4)
    def fetch_block(
        pagina_inicio: int,
        endpoint: str,
        query_params: dict,
        entidade: str,
        pk: list[str],
    ) -> dict:
        api = ClienteComprasGov()
        ingeridos = 0
        api_total = 0
        for pagina in range(pagina_inicio, pagina_inicio + BLOCK_SIZE):
            _, resp = api.request(
                "GET",
                endpoint,
                params={**query_params, "pagina": pagina, "tamanhoPagina": PAGE_SIZE},
            )
            if not isinstance(resp, dict):
                raise RuntimeError(
                    f"A API não retornou resposta válida: {endpoint}, página={pagina}."
                )
            data = [r for r in resp.get("resultado", []) if r is not None]
            api_total = resp.get("totalRegistros", 0)
            if not data:
                break
            write_raw(
                SISTEMA,
                entidade,
                data,
                primary_key=pk,
            )
            ingeridos += len(data)
            if resp.get("paginasRestantes", 0) == 0:
                break
        return {"ingeridos": ingeridos, "api_total": api_total}

    @task
    def validate(results: Any, endpoint: str) -> None:
        total_ingerido = sum(r["ingeridos"] for r in results)
        api_total = results[0]["api_total"] if results else 0
        if total_ingerido != api_total:
            logging.warning(
                "[%s] Divergência: ingeridos=%s api_total=%s",
                endpoint,
                total_ingerido,
                api_total,
            )
        else:
            logging.info("[%s] Validação OK: ingeridos=%s", endpoint, total_ingerido)

    starts_grupo = get_page_starts.override(task_id="get_page_starts_grupo")(
        endpoint=GRUPO_ENDPOINT, query_params=GRUPO_PARAMS
    )
    results_grupo = (
        fetch_block.override(task_id="fetch_block_grupo")
        .partial(
            endpoint=GRUPO_ENDPOINT,
            query_params=GRUPO_PARAMS,
            entidade="grupo_material",
            pk=["codigogrupo"],
        )
        .expand(pagina_inicio=starts_grupo)
    )
    validate.override(task_id="validate_grupo")(
        results=results_grupo, endpoint=GRUPO_ENDPOINT
    )

    starts_classe = get_page_starts.override(task_id="get_page_starts_classe")(
        endpoint=CLASSE_ENDPOINT, query_params=CLASSE_PARAMS
    )
    results_classe = (
        fetch_block.override(task_id="fetch_block_classe")
        .partial(
            endpoint=CLASSE_ENDPOINT,
            query_params=CLASSE_PARAMS,
            entidade="classe_material",
            pk=["codigoclasse"],
        )
        .expand(pagina_inicio=starts_classe)
    )
    validate.override(task_id="validate_classe")(
        results=results_classe, endpoint=CLASSE_ENDPOINT
    )


catalogo_material_grupo_classe_dag()
