"""
DAG de ingestão dos cabeçalhos de contrato ativo do Contratos.gov.br.

Endpoint: GET /api/contrato/ug/{unidade_codigo}
Fonte: https://contratos.comprasnet.gov.br (API aberta, sem autenticação)

Por que varredura por UG?
    O endpoint não pagina, não filtra por período e só existe por unidade
    gestora. A lista de UGs vem da raw que a unidade_contratante_ingest_dag
    grava, e cada UG é uma chamada. O recorte temporal é feito na Silver, pelos
    campos de data do payload (ADR-0021).

Por que blocos em vez de .expand() sobre as UGs?
    São ~3.781 unidades gestoras. Expandir sobre a lista crua criaria uma task
    instance por UG e estouraria o core.max_map_length (1024). Cada bloco de
    BLOCK_SIZE UGs vira uma task que itera internamente (airflow/helpers/batching.py).

Por que uma escrita por UG, e não por bloco?
    Falha no meio de um bloco não descarta o que já veio, e a contagem por UG
    fica no log — é dela que sai o alerta de queda para zero numa UG que tinha
    contratos.

Por que não roda inteira na máquina do desenvolvedor?
    A varredura completa são milhares de chamadas, estimadas em horas. No
    compose, INGEST_MAX_UGS (local.env) trunca a lista e é com essa amostra que
    a DAG é validada; a primeira execução completa acontece em homologação.

Chave primária: id (inteiro, global no sistema).
    No backend warehouse, write_raw faz upsert por `id`; no object_storage a
    raw é append-only e a Silver deduplica por dt_ingest.

Horário: sábado às 08:00 — fora da janela do compras_gov (01:00–07:00) e da
    transformação do MGI (06:00), depois da enumeração diária de UGs das 22:00.
    Semanal porque não há carimbo de alteração no payload: aumentar a
    frequência multiplica o custo sem aumentar a informação
    (docs/notas/contratos-gov-ingestao.md).
"""

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from batching import chunked, limit_local
from cliente_contratos_gov import ClienteContratosGov
from landing_zone import distinct_raw_values, write_raw

SISTEMA = "contratos_gov"
ENTIDADE = "contrato_ativo"
DEPENDENCIA = "unidade_contratante"
PK = ["id"]
BLOCK_SIZE = 25

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="contrato_ativo_ingest_dag",
    schedule="0 8 * * 6",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere o cabeçalho dos contratos ativos por unidade gestora da API "
        "Contratos.gov.br (GET /api/contrato/ug/{codigo}) para "
        "contratos_gov.raw_contrato_ativo. Varredura completa semanal em blocos de UGs."
    ),
    tags=["sistema:contratos_gov", "dominio:contratacoes"],
)
def contrato_ativo_dag() -> None:
    @task
    def get_ug_blocks() -> list[list[str]]:
        try:
            ugs = distinct_raw_values(SISTEMA, DEPENDENCIA, "codigo")
        except Exception as exc:
            raise RuntimeError(
                f"Entidade '{DEPENDENCIA}' ainda não está na zona raw. "
                "Execute unidade_contratante_ingest_dag antes de "
                "contrato_ativo_ingest_dag."
            ) from exc

        if not ugs:
            raise RuntimeError(
                f"Entidade '{DEPENDENCIA}' está na raw, mas sem nenhum código. "
                "Sem UG não há o que varrer: confira a última execução de "
                "unidade_contratante_ingest_dag."
            )

        ugs = limit_local(ugs, "INGEST_MAX_UGS", "UGs")
        blocos = chunked(ugs, BLOCK_SIZE)
        logging.info(
            "Total de UGs a varrer: %s em %s blocos de até %s",
            len(ugs),
            len(blocos),
            BLOCK_SIZE,
        )
        return blocos

    @task(max_active_tis_per_dag=4)
    def ingest_ugs(codigos_ug: list[str]) -> dict:
        api = ClienteContratosGov()
        contratos = 0
        ugs_vazias = []

        for codigo_ug in codigos_ug:
            registros = api.listar_contratos_ug(codigo_ug)

            # UG inexistente e UG sem contrato respondem igual (200 []). Não é
            # erro, mas a queda para zero numa UG que tinha contratos é sinal:
            # por isso a UG vazia volta nomeada, não só contada.
            if not registros:
                logging.info("UG %s: nenhum contrato ativo.", codigo_ug)
                ugs_vazias.append(codigo_ug)
                continue

            write_raw(SISTEMA, ENTIDADE, registros, primary_key=PK)
            logging.info("UG %s: contratos=%s", codigo_ug, len(registros))
            contratos += len(registros)

        return {"contratos": contratos, "ugs_vazias": ugs_vazias}

    @task
    def validate(results: Any) -> int:
        total = sum(r["contratos"] for r in results)
        vazias = [ug for r in results for ug in r["ugs_vazias"]]
        logging.info(
            "Contratos ativos total: contratos=%s blocos=%s ugs_vazias=%s",
            total,
            len(results),
            len(vazias),
        )

        if total == 0:
            raise RuntimeError(
                "Execução sem nenhum contrato ativo em nenhuma das "
                f"{len(vazias)} UGs varridas. A fonte devolver tudo vazio é "
                "anomalia, não realidade: interrompendo antes de a Silver ler "
                "uma raw incompleta."
            )

        return total

    blocos = get_ug_blocks()
    resultados = ingest_ugs.expand(codigos_ug=blocos)
    validate(resultados)


contrato_ativo_dag()
