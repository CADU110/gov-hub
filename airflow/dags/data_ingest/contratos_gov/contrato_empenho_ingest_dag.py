"""Ingestão semanal dos empenhos dos contratos no escopo de detalhamento.

GET /api/contrato/{contrato_id}/empenhos não pagina nem filtra por data. Os
contratos vêm das raws de ativos e inativos, filtrados pelo órgão consumidor.
Uma chamada por contrato é inevitável; expandir uma task por ID excederia o
core.max_map_length, então o Airflow expande apenas blocos (ADR-0021).

A resposta não traz contrato_id: o cliente o injeta a partir da URL antes da
escrita.

A chave é (contrato_id, id). O id é o da nota de empenho, e a mesma nota fica
vinculada a vários contratos: em 80 contratos da UG 201057 (órgão 46000,
2026-10-08), 3.205 linhas tinham 1.627 ids distintos, 715 deles em mais de um
contrato. Com id sozinho, o upsert do warehouse manteria uma linha por nota e
gravaria nela o contrato_id do último contrato lido.

O backend warehouse guarda credor_obj e links como JSON válido;
object_storage mantém os objetos nativos em um arquivo por contrato, para que
as escritas do mesmo run não se sobrescrevam. A Silver fará o recorte temporal
por data_emissao. A varredura completa leva milhares de chamadas e não deve
rodar na máquina do desenvolvedor: INGEST_MAX_CONTRATOS limita a amostra no
compose, e a primeira execução completa deve ocorrer em homologação.
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
ENTIDADE = "contrato_empenho"
PK = ["contrato_id", "id"]
JSON_FIELDS = ["credor_obj", "links"]
BLOCK_SIZE = 25
MAX_BLOCOS = 1024

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="contrato_empenho_ingest_dag",
    schedule="0 13 * * 6",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere empenhos por contrato da API Contratos.gov.br para "
        "contratos_gov.raw_contrato_empenho, em blocos e no escopo dos órgãos consumidores."
    ),
    tags=["sistema:contratos_gov", "dominio:contratacoes"],
)
def contrato_empenho_dag() -> None:
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
        repetidas = 0
        for contrato_id in contrato_ids:
            registros = api.listar_subrecurso(contrato_id, "empenhos")
            if not registros:
                vazios += 1
                logging.info("Contrato %s: nenhum empenho.", contrato_id)
                continue

            # A fonte repete o mesmo vínculo dentro de um contrato (o 93055
            # devolveu 563 linhas para 551 ids em 2026-10-08), e o ON CONFLICT
            # DO UPDATE recusa a mesma chave duas vezes no mesmo comando. Linha
            # idêntica é descartada; mesmo id com conteúdo diferente é anomalia
            # e interrompe antes da escrita.
            unicos: dict[str, dict] = {}
            for registro in registros:
                if str(registro.get("contrato_id")) != contrato_id:
                    raise RuntimeError(
                        f"Contrato {contrato_id}: empenho sem contrato_id da URL."
                    )
                empenho_id = registro.get("id")
                if empenho_id is None:
                    raise RuntimeError(f"Contrato {contrato_id}: empenho sem id.")
                if unicos.setdefault(str(empenho_id), registro) != registro:
                    raise RuntimeError(
                        f"Contrato {contrato_id}: id de empenho {empenho_id!r} "
                        "repetido no lote com conteúdo diferente."
                    )

            lote = list(unicos.values())
            descartadas = len(registros) - len(lote)
            if descartadas:
                repetidas += descartadas
                logging.warning(
                    "Contrato %s: %s linha(s) idêntica(s) repetida(s) na resposta, "
                    "descartada(s) antes da escrita.",
                    contrato_id,
                    descartadas,
                )

            write_raw(
                SISTEMA,
                ENTIDADE,
                lote,
                primary_key=PK,
                run_id=f"{run_id}__contrato_{contrato_id}" if run_id else None,
                json_fields=JSON_FIELDS,
            )
            total += len(lote)
            logging.info("Contrato %s: %s empenhos gravados.", contrato_id, len(lote))

        return {
            "empenhos": total,
            "contratos_vazios": vazios,
            "linhas_repetidas": repetidas,
        }

    @task
    def validate(results: Any) -> int:
        total = sum(resultado["empenhos"] for resultado in results)
        vazios = sum(resultado["contratos_vazios"] for resultado in results)
        repetidas = sum(resultado["linhas_repetidas"] for resultado in results)
        logging.info(
            "Empenhos ingeridos: %s em %s blocos; contratos sem empenho: %s; "
            "linhas repetidas descartadas: %s.",
            total,
            len(results),
            vazios,
            repetidas,
        )
        return total

    blocos = get_contract_blocks()
    validate(ingest_contracts.expand(contrato_ids=blocos))


contrato_empenho_dag()
