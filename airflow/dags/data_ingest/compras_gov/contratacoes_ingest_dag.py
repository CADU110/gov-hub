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
MODALIDADES = [3, 5, 6, 7]  # Concorrência Eletrônica, Pregão, Dispensa, Inexigibilidade

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
    if data_final.date() <= data_inicial.date():
        data_final = data_inicial + timedelta(days=1)
        # Ajuste para garantir que o intervalo seja de pelo menos um dia
    return str(data_inicial.date()), str(data_final.date())


@dag(
    dag_id="contratacoes_ingest_dag",
    schedule="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere contratações públicas (PNCP/Lei 14.133) por modalidade da API do Compras.gov.br para "
        "compras_gov.raw_contratacoes."
    ),
    tags=["sistema:compras_gov", "dominio:contratacoes", "orgao:mgi"],
)
def contratacoes_dag() -> None:
    @task
    def get_page_starts_contratacoes(modalidade: int, **context: dict) -> list[int]:
        data_inicial, data_final = _get_intervalo(context)
        api = ClienteComprasGov()
        _, resp = api.request(
            "GET",
            "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133",
            params={
                "dataPublicacaoPncpInicial": data_inicial,
                "dataPublicacaoPncpFinal": data_final,
                "codigoModalidade": modalidade,
                "pagina": 1,
                "tamanhoPagina": PAGE_SIZE,
            },
        )
        total = resp.get("totalPaginas", 1) if isinstance(resp, dict) else 1
        logging.info(
            "[contratacoes] modalidade=%s %s→%s total_paginas=%s",
            modalidade,
            data_inicial,
            data_final,
            total,
        )
        return page_starts(total, BLOCK_SIZE)

    @task
    def fetch_block_contratacoes(
        pagina_inicio: int, modalidade: int, **context: dict
    ) -> dict:
        data_inicial, data_final = _get_intervalo(context)
        api = ClienteComprasGov()
        ingeridos = 0
        api_total = 0
        for pagina in range(pagina_inicio, pagina_inicio + BLOCK_SIZE):
            time.sleep(1)
            _, resp = api.request(
                "GET",
                "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133",
                params={
                    "dataPublicacaoPncpInicial": data_inicial,
                    "dataPublicacaoPncpFinal": data_final,
                    "codigoModalidade": modalidade,
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
                "contratacoes",
                data,
                primary_key=["idcompra"],
                run_date=date.fromisoformat(data_inicial),
            )
            ingeridos += len(data)
            if resp.get("paginasRestantes", 0) == 0:
                break
        return {"ingeridos": ingeridos, "api_total": api_total}

    @task
    def validate_contratacoes(results: Any, modalidade: int) -> dict[str, int]:
        total_ingerido = sum(r["ingeridos"] for r in results)
        api_total = results[0]["api_total"] if results else 0
        if total_ingerido != api_total:
            logging.warning(
                "[contratacoes] modalidade=%s Divergência: ingeridos=%s api_total=%s",
                modalidade,
                total_ingerido,
                api_total,
            )
        else:
            logging.info(
                "[contratacoes] modalidade=%s Validação OK: ingeridos=%s",
                modalidade,
                total_ingerido,
            )
        return {
            "modalidade": modalidade,
            "ingeridos": total_ingerido,
            "api_total": api_total,
        }

    @task
    def log_total_contratacoes(totais: list[dict[str, int]], **context: dict) -> None:
        data_inicial, data_final = _get_intervalo(context)
        resumo = "\n".join(
            f"modalidade={r['modalidade']} ingeridos={r['ingeridos']} "
            f"api_total={r['api_total']} "
            f"status={'OK' if r['ingeridos'] == r['api_total'] else 'DIVERGENTE'}"
            for r in totais
        )
        logging.info(
            "[contratacoes] Resumo %s→%s\n%s\ntotal_ingerido=%s",
            data_inicial,
            data_final,
            resumo,
            sum(r["ingeridos"] for r in totais),
        )

    # Modalidades sequenciais: m3 → m5 → m6 → m7
    prev_val = None
    totais = []
    for modalidade in MODALIDADES:
        starts = get_page_starts_contratacoes.override(
            task_id=f"get_page_starts_m{modalidade}"
        )(modalidade=modalidade)
        results = (
            fetch_block_contratacoes.override(task_id=f"fetch_block_m{modalidade}")
            .partial(modalidade=modalidade)
            .expand(pagina_inicio=starts)
        )
        val = validate_contratacoes.override(task_id=f"validate_m{modalidade}")(
            results=results, modalidade=modalidade
        )

        if prev_val is not None:
            prev_val >> starts
        prev_val = val
        totais.append(val)

    # O Airflow resolve o XComArg para os resultados antes de executar a task.
    log_total_contratacoes(totais)  # ty: ignore[invalid-argument-type]


contratacoes_dag()
