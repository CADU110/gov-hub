"""
DAG de ingestão das unidades contratantes do Contratos.gov.br.

Endpoint: GET /api/contrato/unidades
Fonte: https://contratos.comprasnet.gov.br (API aberta, sem autenticação)

Por que carga completa?
    O endpoint não tem filtro por período nem paginação. A única estratégia
    viável é buscar a lista inteira a cada execução. O recorte temporal é
    feito depois, na Silver, pelos campos de data presentes nos contratos
    vinculados. (ADR-0021: ingestão agnóstica de motor via write_raw)

Por que falhar em lista vazia?
    A API sempre devolve centenas de UGs com contratos ativos (~3 777 em
    2026-09-15). Uma resposta vazia indica falha na API ou problema de rede,
    não ausência legítima de registros. Gravar uma raw vazia causaria
    reprocessamentos silenciosos na Silver — melhor falhar explicitamente.

Chave primária: codigo (UG SIAFI, 6 dígitos).
    No backend warehouse, write_raw faz upsert por `codigo`, garantindo
    idempotência: duas execuções no mesmo dia não duplicam linhas.
    No backend object_storage, a raw é append-only e a Silver deduplica
    por dt_ingest (ADR-0012).

Horário: 08:00 — após o bloco compras_gov (01:00–07:00) e a transformação
    do MGI (06:00), sem colidir com nenhum pipeline existente.
"""

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from cliente_contratos_gov import ClienteContratosGov
from landing_zone import write_raw

SISTEMA = "contratos_gov"
ENTIDADE = "unidade_contratante"
PK = ["codigo"]

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="unidade_contratante_ingest_dag",
    schedule="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere a lista de unidades contratantes da API Contratos.gov.br "
        "(GET /api/contrato/unidades) para contratos_gov.raw_unidade_contratante. "
        "Endpoint aberto, sem autenticação e sem paginação: carga completa diária."
    ),
    tags=["sistema:contratos_gov", "dominio:contratacoes"],
)
def unidade_contratante_dag() -> None:
    @task
    def fetch_and_ingest() -> int:
        """
        Busca e grava a lista completa de UGs com contratos ativos.

        Uma única chamada GET retorna todos os registros sem paginação.
        Lista vazia é anomalia: causa falha explícita em vez de raw vazia.
        """
        api = ClienteContratosGov()
        registros = api.listar_unidades()

        if not registros:
            raise ValueError(
                "API retornou lista vazia em /api/contrato/unidades. "
                "Anomalia: esperadas centenas de UGs. Interrompendo ingestão."
            )

        logging.info("[%s] %s registros recebidos da API.", ENTIDADE, len(registros))
        write_raw(SISTEMA, ENTIDADE, registros, primary_key=PK)
        logging.info(
            "[%s] Ingestão concluída: %s registros gravados.", ENTIDADE, len(registros)
        )
        return len(registros)

    fetch_and_ingest()


unidade_contratante_dag()
