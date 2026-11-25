import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task

from batching import page_starts
from cliente_compras_gov import ClienteComprasGov
from estado_catalogo import contar_inativos
from landing_zone import COLUNA_INGESTAO, RawIndisponivel, read_raw, write_raw

SISTEMA = "compras_gov"
PAGE_SIZE = 500
BLOCK_SIZE = 15

ENDPOINT = "/modulo-material/4_consultarItemMaterial"
PARAMS = {"statusItem": "true"}
PARAMS_INATIVOS = {"statusItem": "false"}
DAG_INATIVOS = "catalogo_material_item_inativo_ingest_dag"
ENTIDADE = "item_material"
PK = ["codigoitem"]

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="catalogo_material_item_ingest_dag",
    schedule="0 2 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere itens de materiais ativos do catálogo do Compras.gov.br para compras_gov.raw_item_material "
        "e dispara a DAG de itens inativos quando a raw diverge da API."
    ),
    tags=["sistema:compras_gov", "dominio:material", "orgao:mgi"],
)
def catalogo_material_item_dag() -> None:
    @task
    def get_page_starts() -> list[int]:
        api = ClienteComprasGov()
        _, resp = api.request(
            "GET", ENDPOINT, params={**PARAMS, "pagina": 1, "tamanhoPagina": PAGE_SIZE}
        )
        if not isinstance(resp, dict):
            raise RuntimeError(f"A API não retornou resposta válida: {ENDPOINT}.")
        total = resp.get("totalPaginas", 1)
        logging.info("[%s] Total de páginas: %s", ENDPOINT, total)
        return page_starts(total, BLOCK_SIZE)

    @task(max_active_tis_per_dag=4)
    def fetch_block(pagina_inicio: int) -> dict:
        api = ClienteComprasGov()
        ingeridos = 0
        api_total = 0
        for pagina in range(pagina_inicio, pagina_inicio + BLOCK_SIZE):
            _, resp = api.request(
                "GET",
                ENDPOINT,
                params={**PARAMS, "pagina": pagina, "tamanhoPagina": PAGE_SIZE},
            )
            if not isinstance(resp, dict):
                raise RuntimeError(
                    f"A API não retornou resposta válida: {ENDPOINT}, página={pagina}."
                )
            data = [r for r in resp.get("resultado", []) if r is not None]
            api_total = resp.get("totalRegistros", 0)
            if not data:
                break
            write_raw(
                SISTEMA,
                ENTIDADE,
                data,
                primary_key=PK,
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

    @task.short_circuit
    def faltam_inativos() -> bool:
        api = ClienteComprasGov()
        _, resp = api.request(
            "GET", ENDPOINT, params={**PARAMS_INATIVOS, "pagina": 1, "tamanhoPagina": 10}
        )
        api_total = resp.get("totalRegistros") if isinstance(resp, dict) else None
        if api_total is None:
            raise RuntimeError(
                f"[{ENDPOINT}] A API não retornou o total de inativos: {resp!r}."
            )
        try:
            dados = read_raw(
                SISTEMA, ENTIDADE, ["codigoItem", "statusItem", COLUNA_INGESTAO]
            )
            na_raw = contar_inativos(dados, "codigoItem", "statusItem")
        except RawIndisponivel:
            na_raw = 0
        logging.info(
            "[%s] itens inativos: api_total=%s na_raw=%s", ENDPOINT, api_total, na_raw
        )
        return na_raw != api_total

    trigger_inativos = TriggerDagRunOperator(
        task_id="trigger_inativos",
        trigger_dag_id=DAG_INATIVOS,
        wait_for_completion=False,
    )

    starts = get_page_starts()
    results = fetch_block.expand(pagina_inicio=starts)
    validate(results) >> faltam_inativos() >> trigger_inativos


catalogo_material_item_dag()
