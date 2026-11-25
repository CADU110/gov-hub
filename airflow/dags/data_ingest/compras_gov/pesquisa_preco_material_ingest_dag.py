import logging
from collections import Counter
from datetime import date, datetime, timedelta
from typing import Any
from airflow.sdk import dag, task
from batching import chunked
from cliente_compras_gov import ClienteComprasGov
from landing_zone import RawIndisponivel, distinct_raw_values, write_raw

SISTEMA = "compras_gov"
BLOCK_SIZE = 100
JANELA_DIAS = 14
LIMITE_REGISTROS = 20_000
ENDPOINT_PRECO = "/modulo-pesquisa-preco/1_consultarMaterial"
ENTIDADE_PRECO = "pesquisa_preco_material"

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _janela(context: dict) -> dict[str, str]:
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    data_inicial = conf.get("data_inicial")
    data_final = conf.get("data_final")

    if bool(data_inicial) != bool(data_final):
        raise ValueError("Informe data_inicial e data_final juntos em dag_run.conf.")

    if data_inicial and data_final:
        inicio = date.fromisoformat(data_inicial)
        fim = date.fromisoformat(data_final)

        if fim < inicio:
            raise ValueError("data_final não pode ser anterior a data_inicial.")
    else:
        fallback = dag_run.logical_date or dag_run.run_after
        fim = (context.get("data_interval_end") or fallback).date()
        inicio = fim - timedelta(days=JANELA_DIAS)

    return {"dataCompraInicio": inicio.isoformat(), "dataCompraFim": fim.isoformat()}


def _gravar(registros: list[dict], metricas: Counter[str]) -> None:
    """Grava os registros acumulados num Parquet e esvazia a lista."""
    if registros:
        write_raw(SISTEMA, ENTIDADE_PRECO, registros)
        metricas["preco_arquivos"] += 1
        registros.clear()


def _consultar_preco(
    api: ClienteComprasGov,
    params: dict[str, Any],
    acumulados: list[dict],
    metricas: Counter[str],
) -> int:
    recebidos = 0
    api_total = None
    registros: list[dict] = []
    metricas["preco_chamadas"] += 1
    for pagina, api_total in api.iter_pages(ENDPOINT_PRECO, params):
        metricas["preco_paginas"] += 1
        recebidos += len(pagina)
        registros.extend(registro for registro in pagina if registro)
        if len(acumulados) + len(registros) >= LIMITE_REGISTROS:
            _gravar(acumulados, metricas)
            if len(registros) >= LIMITE_REGISTROS:
                _gravar(registros, metricas)
    if api_total is None:
        raise RuntimeError(
            f"A API não retornou resposta válida: {ENDPOINT_PRECO} {params}."
        )
    if recebidos != api_total:
        raise RuntimeError(
            f"Resposta incompleta: {ENDPOINT_PRECO} {params}, "
            f"recebidos={recebidos}, api_total={api_total}."
        )
    acumulados.extend(registros)
    metricas["preco_recebidos"] += recebidos
    return recebidos


@dag(
    dag_id="pesquisa_preco_material_ingest_dag",
    schedule="0 3 * * 0",
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Consulta preços de materiais por PDM, ativo ou inativo, na API de pesquisa de preço do "
        "Compras.gov.br, gravando em compras_gov.raw_pesquisa_preco_material."
    ),
    tags=["sistema:compras_gov", "dominio:pesquisa_preco", "orgao:mgi"],
)
def pesquisa_preco_material_dag() -> None:
    @task
    def get_lotes_pdm(**context: Any) -> list[dict]:
        janela = _janela(context)
        try:
            pdms = sorted(distinct_raw_values(SISTEMA, "pdm_material", "codigoPdm"))
        except Exception as exc:
            raise RuntimeError(
                "A entidade 'pdm_material' precisa estar na zona raw. Execute "
                "catalogo_material_pdm_ingest_dag antes de pesquisa_preco_material_ingest_dag."
            ) from exc
        if not pdms:
            raise RuntimeError("Nenhum PDM em compras_gov.raw_pdm_material.")

        carregados: set[str] = set()
        if (context["dag_run"].conf or {}).get("carga_completa") is True:
            logging.info("carga_completa em dag_run.conf: a raw de preço é ignorada.")
        else:
            try:
                carregados = set(
                    distinct_raw_values(SISTEMA, ENTIDADE_PRECO, "codigoPdm")
                )
            except RawIndisponivel:
                logging.info("%s ainda não está na raw.", ENTIDADE_PRECO)

        novos = [pdm for pdm in pdms if pdm not in carregados]
        conhecidos = [pdm for pdm in pdms if pdm in carregados]
        lotes = [{"pdms": lote, "filtro": {}} for lote in chunked(novos, BLOCK_SIZE)] + [
            {"pdms": lote, "filtro": janela} for lote in chunked(conhecidos, BLOCK_SIZE)
        ]
        logging.info(
            "Pesquisa de preços material: %s PDMs, %s pelo histórico completo, "
            "%s pela janela %s, em %s lotes de até %s",
            len(pdms),
            len(novos),
            len(conhecidos),
            janela,
            len(lotes),
            BLOCK_SIZE,
        )
        return lotes

    @task(max_active_tis_per_dag=2)
    def fetch_preco_material(lote: dict) -> dict:
        api = ClienteComprasGov()
        filtro = lote["filtro"]
        metricas: Counter[str] = Counter(pdms=len(lote["pdms"]))
        acumulados: list[dict] = []

        for codigo_pdm in lote["pdms"]:
            recebidos = _consultar_preco(
                api,
                {"tipo": "codigoPdm", "codigo": codigo_pdm, **filtro},
                acumulados,
                metricas,
            )
            if recebidos:
                metricas["pdms_com_preco"] += 1
        _gravar(acumulados, metricas)

        logging.info("Lote %s: %s", filtro or "histórico completo", dict(metricas))
        return dict(metricas)

    @task
    def validate(results: Any) -> None:
        total: Counter[str] = Counter()
        for metricas in results:
            total.update(metricas)
        logging.info("Pesquisa preço material: blocos=%s %s", len(results), dict(total))

    validate(fetch_preco_material.expand(lote=get_lotes_pdm()))


pesquisa_preco_material_dag()
