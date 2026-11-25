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
ENDPOINT = "/modulo-contratacoes/3_consultarResultadoItensContratacoes_PNCP_14133"
ENTIDADE = "resultado_itens_contratacoes"
PK = ["idcompraitem", "sequencialresultado"]
default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _get_intervalo(context: dict) -> tuple[str, str]:
    dag_run = context["dag_run"]
    fallback = dag_run.logical_date or dag_run.run_after
    data_inicial = context.get("data_interval_start") or fallback
    data_final = context.get("data_interval_end") or fallback
    return str(data_inicial.date()), str(data_final.date())


@dag(
    dag_id="resultado_itens_contratacoes_ingest_dag",
    schedule="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere resultados de itens de contratações (PNCP/Lei 14.133) da API do Compras.gov.br para "
        "compras_gov.raw_resultado_itens_contratacoes."
    ),
    tags=["sistema:compras_gov", "dominio:contratacoes", "orgao:mgi"],
)
def resultado_itens_contratacoes_dag() -> None:
    @task
    def get_page_starts(**context: dict) -> list[int]:
        data_inicial, data_final = _get_intervalo(context)
        api = ClienteComprasGov()
        _, resp = api.request(
            "GET",
            ENDPOINT,
            params={
                "dataResultadoPncpInicial": data_inicial,
                "dataResultadoPncpFinal": data_final,
                "bps": "false",
                "pagina": 1,
                "tamanhoPagina": PAGE_SIZE,
            },
        )
        total = resp.get("totalPaginas", 1) if isinstance(resp, dict) else 1
        logging.info(
            "[%s] %s→%s total_paginas=%s", ENDPOINT, data_inicial, data_final, total
        )
        return page_starts(total, BLOCK_SIZE)

    @task
    def fetch_block(pagina_inicio: int, **context: dict) -> dict:
        data_inicial, data_final = _get_intervalo(context)
        api = ClienteComprasGov()
        ingeridos = 0
        api_total = 0
        for pagina in range(pagina_inicio, pagina_inicio + BLOCK_SIZE):
            time.sleep(1)
            _, resp = api.request(
                "GET",
                ENDPOINT,
                params={
                    "dataResultadoPncpInicial": data_inicial,
                    "dataResultadoPncpFinal": data_final,
                    "bps": "false",
                    "pagina": pagina,
                    "tamanhoPagina": PAGE_SIZE,
                },
            )
            if not isinstance(resp, dict):
                break
            data = [r for r in resp.get("resultado", []) if r is not None]
            api_total = resp.get("totalRegistros", 0)
            if not data:
                break
            write_raw(
                SISTEMA,
                ENTIDADE,
                data,
                primary_key=PK,
                run_date=date.fromisoformat(data_inicial),
            )
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


resultado_itens_contratacoes_dag()
