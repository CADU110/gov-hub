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

ENDPOINT = "/modulo-servico/8_consultarNaturezaDespesaServico"
PARAMS = {"statusNaturezaDespesa": "true"}
ENTIDADE = "natureza_despesa_servico"
PK = ["codigoservico", "codigonaturezadespesa"]

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
    dag_id="catalogo_servico_natureza_despesa_ingest_dag",
    schedule="0 3 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere naturezas de despesa de serviços do catálogo do Compras.gov.br para "
        "compras_gov.raw_natureza_despesa_servico."
    ),
    tags=["sistema:compras_gov", "dominio:servico", "orgao:mgi"],
)
def catalogo_servico_natureza_despesa_dag() -> None:
    @task
    def get_page_starts() -> list[int]:
        api = ClienteComprasGov()
        _, resp = api.request(
            "GET", ENDPOINT, params={**PARAMS, "pagina": 1, "tamanhoPagina": PAGE_SIZE}
        )
        total = resp.get("totalPaginas", 1) if isinstance(resp, dict) else 1
        logging.info("[%s] Total de páginas: %s", ENDPOINT, total)
        return page_starts(total, BLOCK_SIZE)

    @task
    def fetch_block(pagina_inicio: int, **context: dict) -> dict:
        api = ClienteComprasGov()
        run_date = _run_date(context)
        ingeridos = 0
        api_total = 0
        for pagina in range(pagina_inicio, pagina_inicio + BLOCK_SIZE):
            time.sleep(1)
            _, resp = api.request(
                "GET",
                ENDPOINT,
                params={**PARAMS, "pagina": pagina, "tamanhoPagina": PAGE_SIZE},
            )
            if not isinstance(resp, dict):
                break
            data = [r for r in resp.get("resultado", []) if r is not None]
            api_total = resp.get("totalRegistros", 0)
            if not data:
                break
            write_raw(SISTEMA, ENTIDADE, data, primary_key=PK, run_date=run_date)
            ingeridos += len(data)
            if resp.get("paginasRestantes", 0) == 0:
                break
        return {"ingeridos": ingeridos, "api_total": api_total}

    @task
    def validate(results: Any) -> None:
        total_ingerido = sum(r["ingeridos"] for r in results)
        api_total = results[0]["api_total"] if results else 0
        if total_ingerido != api_total:
            logging.warning(
                "[%s] Divergência: ingeridos=%s api_total=%s",
                ENDPOINT,
                total_ingerido,
                api_total,
            )
        else:
            logging.info("[%s] Validação OK: ingeridos=%s", ENDPOINT, total_ingerido)

    starts = get_page_starts()
    results = fetch_block.expand(pagina_inicio=starts)
    validate(results)


catalogo_servico_natureza_despesa_dag()
