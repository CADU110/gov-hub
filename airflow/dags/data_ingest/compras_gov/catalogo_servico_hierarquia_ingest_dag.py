import logging
import time
from datetime import date, datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from batching import page_starts
from cliente_compras_gov import ClienteComprasGov
from landing_zone import write_raw

SISTEMA = "compras_gov"
PAGE_SIZE = 500
BLOCK_SIZE = 15

ENDPOINTS = [
    (
        "/modulo-servico/1_consultarSecaoServico",
        {"statusSecao": "true"},
        "secao_servico",
        ["codigosecao"],
    ),
    (
        "/modulo-servico/2_consultarDivisaoServico",
        {"statusDivisao": "true"},
        "divisao_servico",
        ["codigodivisao"],
    ),
    (
        "/modulo-servico/3_consultarGrupoServico",
        {"statusGrupo": "true"},
        "grupo_servico",
        ["codigogrupo"],
    ),
    (
        "/modulo-servico/4_consultarClasseServico",
        {},
        "classe_servico",
        ["codigoclasse"],
    ),
    (
        "/modulo-servico/5_consultarSubClasseServico",
        {"statusSubclasse": "true"},
        "subclasse_servico",
        ["codigosubclasse"],
    ),
]

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _run_date(context: dict) -> date:
    dag_run = context["dag_run"]
    fallback = dag_run.logical_date or dag_run.run_after
    inicio = context.get("data_interval_start") or fallback
    return inicio.date()


@dag(
    dag_id="catalogo_servico_hierarquia_ingest_dag",
    schedule="0 3 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere a hierarquia de seções, divisões, grupos, classes e subclasses de serviços do catálogo do "
        "Compras.gov.br para as respectivas tabelas raw_*_servico."
    ),
    tags=["sistema:compras_gov", "dominio:servico", "orgao:mgi"],
)
def catalogo_servico_hierarquia_dag() -> None:
    @task
    def get_page_starts(endpoint: str, query_params: dict) -> list[int]:
        api = ClienteComprasGov()
        _, resp = api.request(
            "GET",
            endpoint,
            params={**query_params, "pagina": 1, "tamanhoPagina": PAGE_SIZE},
        )
        total = resp.get("totalPaginas", 1) if isinstance(resp, dict) else 1
        logging.info("[%s] Total de páginas: %s", endpoint, total)
        return page_starts(total, BLOCK_SIZE)

    @task
    def fetch_block(
        pagina_inicio: int,
        endpoint: str,
        query_params: dict,
        entidade: str,
        pk: list[str],
        **context: dict,
    ) -> dict:
        run_date = _run_date(context)
        api = ClienteComprasGov()
        ingeridos = 0
        api_total = 0
        for pagina in range(pagina_inicio, pagina_inicio + BLOCK_SIZE):
            time.sleep(1)
            _, resp = api.request(
                "GET",
                endpoint,
                params={**query_params, "pagina": pagina, "tamanhoPagina": PAGE_SIZE},
            )
            if not isinstance(resp, dict):
                break
            data = [r for r in resp.get("resultado", []) if r is not None]
            api_total = resp.get("totalRegistros", 0)
            if not data:
                break
            write_raw(SISTEMA, entidade, data, primary_key=pk, run_date=run_date)
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

    for endpoint, query_params, entidade, pk in ENDPOINTS:
        slug = entidade.replace("_", "-")
        starts = get_page_starts.override(task_id=f"get_page_starts_{slug}")(
            endpoint=endpoint, query_params=query_params
        )
        results = (
            fetch_block.override(task_id=f"fetch_block_{slug}")
            .partial(
                endpoint=endpoint, query_params=query_params, entidade=entidade, pk=pk
            )
            .expand(pagina_inicio=starts)
        )
        validate.override(task_id=f"validate_{slug}")(results=results, endpoint=endpoint)


catalogo_servico_hierarquia_dag()
