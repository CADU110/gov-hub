"""Ingestão semanal das despesas acessórias dos contratos do escopo.

GET /api/contrato/{contrato_id}/despesas_acessorias não pagina, e a ingestão é
a carga completa que a issue #31 define. O código-fonte aceita
dt_alteracao_min e dt_alteracao_max opcionais, mas o filtro não foi testado
contra a API e fica fora da DAG. Os contratos vêm das raws de
ativos e inativos, filtrados pelo órgão consumidor (Variable
contratos_gov_escopo_orgaos). Uma chamada por contrato é inevitável; expandir
uma task por ID excederia o core.max_map_length, então o Airflow expande apenas
blocos (ADR-0021).

Nenhuma resposta real com dados foi observada. A issue registra listas vazias
nos seis contratos consultados em 2026-09-15, e a estrutura vem do código-fonte
público (Contratodespesaacessoria::despesaAcessoriaAPI), onde id é a chave
serial da tabela de origem. A raw guarda o payload como veio, e a chave é
conferida a cada lote para que uma divergência entre fonte e código interrompa
a execução em vez de duplicar linhas. Despesa acessória sem id ou com id repetido no mesmo lote falha porque
o ON CONFLICT do warehouse recusa a mesma chave duas vezes no mesmo comando.

O contrato_id da URL deve coincidir com o do payload em toda linha: a
divergência falha antes da escrita, porque atribuiria a despesa a outro
contrato. Contrato sem despesa acessória é o caso comum e não interrompe o
bloco. No object_storage, cada contrato tem arquivo próprio, para que as
escritas do mesmo run não se sobrescrevam.

A varredura completa leva milhares de chamadas e não deve rodar na máquina do
desenvolvedor: INGEST_MAX_CONTRATOS (definida só no local.env) limita a amostra
no compose, e a primeira execução completa ocorre em homologação.

A agenda é sábado 17:30: a janela dos sub-recursos começa às 12:00 e escalona
de 30 em 30 minutos na ordem das fases da issue #15, depois dos terceirizados.
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
ENTIDADE = "contrato_despesa_acessoria"
RECURSO = "despesas_acessorias"
PK = ["id"]
BLOCK_SIZE = 25
MAX_BLOCOS = 1024

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


def _validar_lote(contrato_id: str, registros: list[dict]) -> None:
    """Falha antes da escrita se o lote apontar para outro contrato ou
    repetir a chave primária."""
    vistos: set[str] = set()
    for registro in registros:
        if str(registro.get("contrato_id")) != contrato_id:
            raise RuntimeError(
                f"Contrato {contrato_id}: despesa acessória com contrato_id "
                f"divergente da URL ({registro.get('contrato_id')!r})."
            )
        despesa_id = registro.get("id")
        if despesa_id is None:
            raise RuntimeError(f"Contrato {contrato_id}: despesa acessória sem id.")
        if str(despesa_id) in vistos:
            raise RuntimeError(
                f"Contrato {contrato_id}: id de despesa acessória {despesa_id!r} "
                "repetido no lote."
            )
        vistos.add(str(despesa_id))


@dag(
    dag_id="contrato_despesa_acessoria_ingest_dag",
    schedule="30 17 * * 6",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere as despesas acessórias por contrato da API Contratos.gov.br "
        "para contratos_gov.raw_contrato_despesa_acessoria, em blocos e no "
        "escopo dos órgãos consumidores."
    ),
    tags=["sistema:contratos_gov", "dominio:contratacoes"],
)
def contrato_despesa_acessoria_dag() -> None:
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
            # Sem contexto (chamada direta em teste), write_raw gera um
            # identificador único; em execução, cada contrato precisa de um
            # arquivo próprio no object_storage.
            run_id = None
        total = 0
        vazios = 0
        for contrato_id in contrato_ids:
            registros = api.listar_subrecurso(contrato_id, RECURSO)
            if not registros:
                vazios += 1
                logging.info("Contrato %s: nenhuma despesa acessória.", contrato_id)
                continue

            _validar_lote(contrato_id, registros)

            write_raw(
                SISTEMA,
                ENTIDADE,
                registros,
                primary_key=PK,
                run_id=f"{run_id}__contrato_{contrato_id}" if run_id else None,
            )
            total += len(registros)
            logging.info(
                "Contrato %s: %s despesas acessórias gravadas.",
                contrato_id,
                len(registros),
            )

        return {"despesas_acessorias": total, "contratos_vazios": vazios}

    @task
    def validate(results: Any) -> int:
        total = sum(resultado["despesas_acessorias"] for resultado in results)
        vazios = sum(resultado["contratos_vazios"] for resultado in results)
        logging.info(
            "Despesas acessórias ingeridas: %s em %s blocos; contratos sem "
            "despesa acessória: %s.",
            total,
            len(results),
            vazios,
        )
        return total

    blocos = get_contract_blocks()
    validate(ingest_contracts.expand(contrato_ids=blocos))


contrato_despesa_acessoria_dag()
