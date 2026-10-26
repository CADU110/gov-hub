"""Ingestão semanal do cronograma dos contratos no escopo de detalhamento.

GET /api/contrato/{contrato_id}/cronograma não pagina nem filtra por data: a
única forma de ingerir é a carga completa, com o recorte temporal feito depois,
na Silver, por vencimento e por mesref/anoref. Os contratos vêm das raws de
ativos e inativos, filtrados pelo órgão consumidor (Variable
contratos_gov_escopo_orgaos). Uma chamada por contrato é inevitável; expandir
uma task por ID excederia o core.max_map_length, então o Airflow expande apenas
blocos (ADR-0021).

Cada linha é uma parcela do cronograma físico-financeiro, e a chave é só id,
como o catálogo declara. O volume varia muito por contrato (0 a 97 parcelas na
amostra de 2026-09-15): cronograma vazio é caso comum, conta como contrato
vazio e não interrompe o bloco. As parcelas de um contrato vão numa escrita só,
não uma por parcela.

Nenhum campo do payload é objeto ou lista, ao contrário do item, então
write_raw é chamado sem json_fields. valor chega como texto pt-BR
("5.335.214,10") e observacao pode trazer o objeto do termo inteiro: os dois
seguem para a raw como vieram, porque converter e truncar são conta da Silver.
No object_storage, cada contrato tem arquivo próprio, para que as escritas do
mesmo run não se sobrescrevam.

A varredura completa leva milhares de chamadas e horas de execução, e não deve
rodar na máquina do desenvolvedor: INGEST_MAX_CONTRATOS (definida só no
local.env) limita a amostra no compose, e a primeira execução completa ocorre
em homologação, depois do teste de carga da feature. Para validar localmente, a
raw de ativos e inativos precisa conter contratos de um órgão da Variable de
escopo (docs/notas/contratos-gov-ingestao.md).

A agenda é sábado 14:00: a janela dos sub-recursos começa às 12:00 e escalona
de 30 em 30 minutos na ordem das fases.
"""

import logging
from datetime import datetime, timedelta
from math import ceil
from typing import Any

from airflow.sdk import dag, get_current_context, task

from batching import chunked, limit_local
from cliente_contratos_gov import ClienteContratosGov
from contratos_gov_scope import ids_contratos_no_escopo, orgaos_no_escopo
from landing_zone import write_raw

SISTEMA = "contratos_gov"
ENTIDADE = "contrato_cronograma"
RECURSO = "cronograma"
PK = ["id"]
BLOCK_SIZE = 25
MAX_BLOCOS = 1024

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="contrato_cronograma_ingest_dag",
    schedule="0 14 * * 6",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere o cronograma por contrato da API Contratos.gov.br para "
        "contratos_gov.raw_contrato_cronograma, em blocos e no escopo dos "
        "órgãos consumidores."
    ),
    tags=["sistema:contratos_gov", "dominio:contratacoes"],
)
def contrato_cronograma_dag() -> None:
    @task
    def get_contract_blocks() -> list[list[str]]:
        ids = ids_contratos_no_escopo(orgaos_no_escopo())
        ids = limit_local(ids, "INGEST_MAX_CONTRATOS", "contratos")
        if not ids:
            raise RuntimeError(
                "INGEST_MAX_CONTRATOS deixou a lista vazia: configure um limite "
                "positivo para validar a ingestão local."
            )
        # O tamanho cresce apenas quando necessário para manter o fan-out
        # abaixo do limite padrão do Airflow, mesmo com vários órgãos no escopo.
        tamanho = max(BLOCK_SIZE, ceil(len(ids) / MAX_BLOCOS))
        blocos = chunked(ids, tamanho)
        logging.info("Contratos no escopo: %s em %s blocos.", len(ids), len(blocos))
        return blocos

    @task(max_active_tis_per_dag=4)
    def ingest_contracts(contrato_ids: list[str]) -> dict[str, int]:
        api = ClienteContratosGov()
        try:
            run_id = str(get_current_context()["run_id"])
        except RuntimeError:
            # A chamada direta em testes não tem contexto; write_raw já gera um
            # identificador único nesse caso. Em execução, cada contrato precisa
            # de um arquivo próprio no object_storage para não sobrescrever os
            # demais contratos do mesmo run.
            run_id = None
        total = 0
        vazios = 0
        for contrato_id in contrato_ids:
            registros = api.listar_subrecurso(contrato_id, RECURSO)
            if not registros:
                # 200 [] é resposta legítima para contrato sem cronograma.
                vazios += 1
                logging.info("Contrato %s: nenhuma parcela.", contrato_id)
                continue

            write_raw(
                SISTEMA,
                ENTIDADE,
                registros,
                primary_key=PK,
                run_id=f"{run_id}__contrato_{contrato_id}" if run_id else None,
            )
            total += len(registros)
            logging.info(
                "Contrato %s: %s parcelas gravadas.", contrato_id, len(registros)
            )

        return {"parcelas": total, "contratos_vazios": vazios}

    @task
    def validate(results: Any) -> int:
        total = sum(resultado["parcelas"] for resultado in results)
        vazios = sum(resultado["contratos_vazios"] for resultado in results)
        logging.info(
            "Parcelas ingeridas: %s em %s blocos; contratos sem cronograma: %s.",
            total,
            len(results),
            vazios,
        )
        return total

    blocos = get_contract_blocks()
    validate(ingest_contracts.expand(contrato_ids=blocos))


contrato_cronograma_dag()
