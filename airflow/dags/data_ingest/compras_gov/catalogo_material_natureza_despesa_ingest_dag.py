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

ENDPOINT = "/modulo-material/5_consultarMaterialNaturezaDespesa"
# Sem filtro de status: a API declara statusNaturezaDespesa como Boolean, mas o
# backend falha ao vinculá-lo e devolve HTTP 400 para qualquer valor
# ("Could not convert 'java.lang.Boolean' to 'java.lang.String'"). Como
# ClienteBase.request levanta exceção após os retries, enviar o parâmetro
# derrubava a DAG em toda execução. Sem ele o endpoint responde normalmente — e
# a coluna statusNaturezaDespesa vem nula em todos os registros de qualquer jeito.
PARAMS: dict[str, str] = {}
ENTIDADE = "natureza_despesa_material"
PK = ["codigopdm", "codigonaturezadespesa"]

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="catalogo_material_natureza_despesa_ingest_dag",
    schedule="0 2 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere naturezas de despesa de materiais do catálogo do Compras.gov.br para "
        "compras_gov.raw_natureza_despesa_material."
    ),
    tags=["sistema:compras_gov", "dominio:material", "orgao:mgi"],
)
def catalogo_material_natureza_despesa_dag() -> None:
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

    starts = get_page_starts()
    results = fetch_block.expand(pagina_inicio=starts)
    validate(results)


catalogo_material_natureza_despesa_dag()
