import logging
import time
from datetime import date, datetime, timedelta
from typing import Any
import polars as pl

from airflow.sdk import dag, task

from cliente_compras_gov import ClienteComprasGov
from landing_zone import read_raw, write_raw

SISTEMA = "compras_gov"

# Combinações da raw de itens de ARP sobre as quais esta DAG pagina.
COLUNAS_ATA = ["numeroAtaRegistroPreco", "codigoUnidadeGerenciadora"]
COLUNAS_ITEM = COLUNAS_ATA + ["numeroItem"]
BLOCK_SIZE = 40
PAGE_SIZE = 500
REQUEST_DELAY_SECONDS = 1
REQUEST_TIMEOUT_SECONDS = 30
TASK_TIMEOUT = timedelta(minutes=20)

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _get_run_date(context: dict) -> date:
    dag_run = context["dag_run"]
    fallback = dag_run.logical_date or dag_run.run_after
    data_inicial = context.get("data_interval_start") or fallback
    return data_inicial.date()


def _get_dt_ingest_interval(context: dict) -> tuple[date, date]:
    """
    Solução temporária para limitar a ingestão de detalhes de ARPs
    a um intervalo de datas de ingestão
    """
    conf = context["dag_run"].conf or {}

    data_inicial = conf.get("data_inicial")
    data_final = conf.get("data_final")

    if bool(data_inicial) != bool(data_final):
        raise ValueError("Informe data_inicial e data_final juntos em dag_run.conf.")

    if data_inicial and data_final:
        inicio = date.fromisoformat(data_inicial)
        fim = date.fromisoformat(data_final)

        if fim < inicio:
            raise ValueError("data_final não pode ser anterior a data_inicial.")

        return inicio, fim

    dados = read_raw(SISTEMA, "arp_item", ["dt_ingest"])

    if dados.is_empty():
        raise RuntimeError("Não há registros de arp_item na zona raw.")

    ultimo_dt = dados.select(pl.col("dt_ingest").max()).item()

    if ultimo_dt is None:
        raise RuntimeError("Não foi possível determinar o último dt_ingest.")

    if isinstance(ultimo_dt, datetime):
        ultimo_dia = ultimo_dt.date()
    else:
        ultimo_dia = date.fromisoformat(str(ultimo_dt)[:10])

    logging.info(
        "Nenhum intervalo informado; usando o último dia de ingestão: %s",
        ultimo_dia,
    )

    return ultimo_dia, ultimo_dia


def _linhas_distintas(intervalo: dict[str, str], colunas: list[str]) -> list[list[str]]:
    dados = read_raw(
        SISTEMA,
        "arp_item",
        [*colunas, "dt_ingest"],
    ).with_columns(
        pl.col("dt_ingest").cast(pl.Utf8).str.slice(0, 10).alias("_data_ingest")
    )

    dados = dados.filter(
        (pl.col("_data_ingest") >= intervalo["inicio"])
        & (pl.col("_data_ingest") <= intervalo["fim"])
    )

    antes = dados.height
    dados = dados.drop_nulls(subset=colunas)
    if dados.height < antes:
        logging.warning(
            "ARP detalhes: %s linha(s) ignoradas por chave nula em %s",
            antes - dados.height,
            colunas,
        )

    dados = dados.unique(subset=colunas).sort(colunas)

    logging.info(
        "ARP detalhes: %s linha(s) distintas de %s até %s",
        dados.height,
        intervalo["inicio"],
        intervalo["fim"],
    )

    return [
        [str(valor) for valor in linha] for linha in dados.select(colunas).iter_rows()
    ]


def _em_blocos(linhas: list[list[str]]) -> list[list[list[str]]]:
    return [linhas[i : i + BLOCK_SIZE] for i in range(0, len(linhas), BLOCK_SIZE)]


def _fetch_all_pages_strict(
    api: ClienteComprasGov,
    endpoint: str,
    params: dict[str, str],
    timeout: int = REQUEST_TIMEOUT_SECONDS,
) -> list[dict]:
    """Busca todas as páginas e deixa a task falhar se uma página falhar."""
    registros: list[dict] = []
    pagina = 1

    while True:
        time.sleep(REQUEST_DELAY_SECONDS)
        _, resposta = api.request(
            "GET",
            endpoint,
            params={
                **params,
                "pagina": pagina,
                "tamanhoPagina": PAGE_SIZE,
            },
            timeout=timeout,
        )
        if not isinstance(resposta, dict):
            raise RuntimeError(f"Resposta inválida da API em {endpoint}, página {pagina}")

        registros.extend(
            registro for registro in resposta.get("resultado", []) if registro is not None
        )
        if resposta.get("paginasRestantes", 0) == 0:
            return registros

        pagina += 1


@dag(
    dag_id="arp_detalhes_ingest_dag",
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere detalhes de unidades, adesões e empenhos das ARPs já ingeridas, a partir da API do Compras.gov.br, "
        "para compras_gov.raw_arp_unidades_item, raw_arp_adesoes_item e raw_arp_empenhos_saldo."
    ),
    tags=["sistema:compras_gov", "dominio:arp", "orgao:mgi"],
)
def arp_detalhes_dag() -> None:
    @task
    def get_intervalo(**context: dict) -> dict[str, str]:
        inicio, fim = _get_dt_ingest_interval(context)
        return {"inicio": inicio.isoformat(), "fim": fim.isoformat()}

    @task
    def get_blocos_itens(intervalo: Any) -> list[list[list[str]]]:
        linhas = _linhas_distintas(intervalo, COLUNAS_ITEM)
        blocos = _em_blocos(linhas)
        logging.info(
            "ARP detalhes itens: %s itens em %s blocos", len(linhas), len(blocos)
        )
        return blocos

    @task
    def get_blocos_atas(intervalo: Any) -> list[list[list[str]]]:
        linhas = _linhas_distintas(intervalo, COLUNAS_ATA)
        blocos = _em_blocos(linhas)
        logging.info(
            "ARP detalhes empenhos: %s atas em %s blocos", len(linhas), len(blocos)
        )
        return blocos

    @task(max_active_tis_per_dag=2, execution_timeout=TASK_TIMEOUT)
    def fetch_unidades_adesoes(bloco: list[list[str]], **context: dict) -> dict:
        api = ClienteComprasGov()
        run_date = _get_run_date(context)
        unidades_bloco: list[dict] = []
        adesoes_bloco: list[dict] = []

        for ata, ug, item in bloco:
            ctx = {
                "numeroAtaRegistroPreco": ata,
                "codigoUnidadeGerenciadora": ug,
                "numeroItem": item,
            }
            params = {
                "numeroAta": ata,
                "unidadeGerenciadora": ug,
                "numeroItem": item,
            }
            unidades = _fetch_all_pages_strict(
                api, "/modulo-arp/3_consultarUnidadesItem", params
            )
            unidades_bloco.extend({**ctx, **r} for r in unidades)

            adesoes = _fetch_all_pages_strict(
                api, "/modulo-arp/5_consultarAdesoesItem", params
            )
            adesoes_bloco.extend({**ctx, **r} for r in adesoes)

        # Grava só no fim do bloco, se a task falhar no meio
        if unidades_bloco:
            write_raw(SISTEMA, "arp_unidades_item", unidades_bloco, run_date=run_date)
        if adesoes_bloco:
            write_raw(SISTEMA, "arp_adesoes_item", adesoes_bloco, run_date=run_date)

        return {"unidades": len(unidades_bloco), "adesoes": len(adesoes_bloco)}

    @task(max_active_tis_per_dag=2, execution_timeout=TASK_TIMEOUT)
    def fetch_empenhos(bloco: list[list[str]], **context: dict) -> dict:
        api = ClienteComprasGov()
        run_date = _get_run_date(context)
        empenhos_bloco: list[dict] = []

        for ata, ug in bloco:
            ctx = {
                "numeroAtaRegistroPreco": ata,
                "codigoUnidadeGerenciadora": ug,
            }
            empenhos = _fetch_all_pages_strict(
                api,
                "/modulo-arp/4_consultarEmpenhosSaldoItem",
                {"numeroAta": ata, "unidadeGerenciadora": ug},
            )
            empenhos_bloco.extend({**ctx, **r} for r in empenhos)

        if empenhos_bloco:
            write_raw(SISTEMA, "arp_empenhos_saldo", empenhos_bloco, run_date=run_date)

        return {"empenhos": len(empenhos_bloco)}

    @task
    def validate_itens(results: Any) -> None:
        total_unidades = sum(r["unidades"] for r in results)
        total_adesoes = sum(r["adesoes"] for r in results)
        logging.info(
            "ARP detalhes itens: unidades=%s adesoes=%s", total_unidades, total_adesoes
        )

    @task
    def validate_empenhos(results: Any) -> None:
        total = sum(r["empenhos"] for r in results)
        logging.info("ARP detalhes empenhos: total=%s", total)

    intervalo = get_intervalo()
    blocos_itens = get_blocos_itens(intervalo)
    blocos_atas = get_blocos_atas(intervalo)

    validate_itens(fetch_unidades_adesoes.expand(bloco=blocos_itens))
    validate_empenhos(fetch_empenhos.expand(bloco=blocos_atas))


arp_detalhes_dag()
