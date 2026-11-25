import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from batching import page_starts
from cliente_compras_gov import ClienteComprasGov
from landing_zone import write_raw

SISTEMA = "compras_gov"
PAGE_SIZE = 500
BLOCK_SIZE = 15

ENDPOINT = "/modulo-uasg/1_consultarUasg"
PARAMS = {"statusUasg": "true"}
ENTIDADE = "uasg"
PK = ["codigouasg"]

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="uasg_ingest_dag",
    schedule="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere UASGs (unidades gestoras) da API do Compras.gov.br para a zona raw."
    ),
    tags=["sistema:compras_gov", "camada:raw", "dominio:uasg"],
)
def uasg_dag() -> None:
    @task
    def get_page_starts() -> list[int]:
        api = ClienteComprasGov()
        _, resp = api.request(
            "GET", ENDPOINT, params={**PARAMS, "pagina": 1, "tamanhoPagina": PAGE_SIZE}
        )
        total = resp.get("totalPaginas", 1) if isinstance(resp, dict) else 1
        logging.info("[%s] Total de páginas: %s", ENDPOINT, total)
        return page_starts(total, BLOCK_SIZE)

    @task(max_active_tis_per_dag=1)
    def fetch_block(pagina_inicio: int) -> dict:
        api = ClienteComprasGov()
        total_registros = 0
        # None distingue resposta sem total de uma consulta válida vazia.
        api_total = None
        for pagina in range(pagina_inicio, pagina_inicio + BLOCK_SIZE):
            time.sleep(3)
            _, resp = api.request(
                "GET",
                ENDPOINT,
                params={**PARAMS, "pagina": pagina, "tamanhoPagina": PAGE_SIZE},
            )
            if not isinstance(resp, dict):
                api_total = None
                break
            api_total = resp.get("totalRegistros")
            if api_total is None:
                break
            data = [r for r in resp.get("resultado", []) if r is not None]
            if not data:
                break
            write_raw(
                SISTEMA,
                ENTIDADE,
                data,
                primary_key=PK,
            )
            total_registros += len(data)
            if resp.get("paginasRestantes", 0) == 0:
                break
        if api_total is None:
            raise RuntimeError(
                f"[{ENDPOINT}] A API não retornou resposta válida: página={pagina}."
            )
        return {"registros": total_registros, "api_total": api_total}

    @task
    def validate(results: Any) -> None:
        results = list(results)
        total_registros = sum(result["registros"] for result in results)
        if not results or any(result["api_total"] is None for result in results):
            raise RuntimeError(f"[{ENDPOINT}] A API não retornou um total válido.")
        api_total = results[0]["api_total"]
        if total_registros != api_total:
            raise RuntimeError(
                f"[{ENDPOINT}] Resposta incompleta: "
                f"gravados={total_registros}, api_total={api_total}."
            )
        else:
            logging.info(
                "[%s] Validação OK: total de registros=%s", ENDPOINT, total_registros
            )

    starts = get_page_starts()
    results = fetch_block.expand(pagina_inicio=starts)
    validate(results)


uasg_dag()
