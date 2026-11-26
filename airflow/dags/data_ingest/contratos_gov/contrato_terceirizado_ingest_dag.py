"""Ingestão semanal dos terceirizados alocados nos contratos do escopo.

GET /api/contrato/{contrato_id}/terceirizados não pagina nem filtra por data:
a única forma de ingerir é a carga completa, e o recorte temporal fica para a
Silver (data_inicio e data_fim). Os contratos vêm das raws de ativos e
inativos, filtrados pelo órgão consumidor (Variable contratos_gov_escopo_orgaos);
se essas raws ainda não existem, ids_contratos_no_escopo falha dizendo qual DAG
rodar antes. Uma chamada por contrato é inevitável; expandir uma task por ID
excederia o core.max_map_length, então o Airflow expande apenas blocos
(ADR-0021).

Dado pessoal (issue #30): cada linha é um trabalhador, com nome e CPF mascarado
em `usuario`, salário e benefícios. O catálogo classifica a entidade como
`pessoal`, e nada do payload aparece em log ou mensagem de erro, porque o log
da task é lido por quem não tem acesso à raw (ADR-0020). As mensagens citam só
o contrato, o id do registro e contagens.

A API devolveu lista vazia para toda a amostra do levantamento, então a
estrutura vem do código-fonte (Contratoterceirizado::terceirizadoAPI). Valores
como salário e custo chegam como texto pt-BR ("1.200,25") e seguem assim para a
raw: converter é da Silver. Contrato sem terceirizado (`200 []`) conta como
contrato vazio e não interrompe o bloco. Os terceirizados de um contrato vão
numa escrita só, sem json_fields, porque o payload é plano.

Cada linha é um trabalhador alocado, e a chave é só id, como o catálogo
declara (hipótese até a API devolver dado). O payload já traz contrato_id; o
valor da URL deve coincidir com ele em toda linha, e a divergência falha antes
da escrita. ClienteContratosGov.listar_subrecurso confere (e preenche quando
falta); _validar_lote repete a conferência como segunda barreira, junto da
checagem de id. Registro sem id ou com id repetido no mesmo lote interrompe a
execução: o ON CONFLICT do warehouse recusa a mesma chave duas vezes no mesmo
comando, e descartar esconderia o caso. No object_storage, cada contrato tem
arquivo próprio, para que as escritas do mesmo run não se sobrescrevam.

A varredura completa leva milhares de chamadas e horas de execução, e não deve
rodar na máquina do desenvolvedor: INGEST_MAX_CONTRATOS (definida só no
local.env) limita a amostra no compose, e a primeira execução completa ocorre
em homologação, depois do teste de carga da feature.

A agenda é sábado 17:00: a janela dos sub-recursos começa às 12:00 e escalona
de 30 em 30 minutos na ordem das fases da issue #15; terceirizados vêm entre
prepostos (16:30) e despesas acessórias (17:30).
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
ENTIDADE = "contrato_terceirizado"
RECURSO = "terceirizados"
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
    repetir a chave primária. As mensagens não carregam o payload."""
    vistos: set[str] = set()
    for posicao, registro in enumerate(registros):
        if str(registro.get("contrato_id")) != contrato_id:
            raise RuntimeError(
                f"Contrato {contrato_id}: terceirizado na posição {posicao} com "
                f"contrato_id divergente da URL ({registro.get('contrato_id')!r})."
            )
        terceirizado_id = registro.get("id")
        if terceirizado_id is None:
            raise RuntimeError(
                f"Contrato {contrato_id}: terceirizado na posição {posicao} sem id."
            )
        if str(terceirizado_id) in vistos:
            raise RuntimeError(
                f"Contrato {contrato_id}: id de terceirizado {terceirizado_id!r} "
                "repetido no lote."
            )
        vistos.add(str(terceirizado_id))


@dag(
    dag_id="contrato_terceirizado_ingest_dag",
    schedule="0 17 * * 6",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere os terceirizados alocados em cada contrato da API "
        "Contratos.gov.br para contratos_gov.raw_contrato_terceirizado, em "
        "blocos e no escopo dos órgãos consumidores. Dado pessoal."
    ),
    tags=["sistema:contratos_gov", "dominio:contratacoes"],
)
def contrato_terceirizado_dag() -> None:
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
                # 200 [] é resposta legítima para contrato sem terceirizado.
                vazios += 1
                logging.info("Contrato %s: nenhum terceirizado.", contrato_id)
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
            # Só a contagem: o payload tem nome, CPF e salário.
            logging.info(
                "Contrato %s: %s terceirizados gravados.", contrato_id, len(registros)
            )

        return {"terceirizados": total, "contratos_vazios": vazios}

    @task
    def validate(results: Any) -> int:
        total = sum(resultado["terceirizados"] for resultado in results)
        vazios = sum(resultado["contratos_vazios"] for resultado in results)
        logging.info(
            "Terceirizados ingeridos: %s em %s blocos; contratos sem terceirizado: %s.",
            total,
            len(results),
            vazios,
        )
        return total

    blocos = get_contract_blocks()
    validate(ingest_contracts.expand(contrato_ids=blocos))


contrato_terceirizado_dag()
