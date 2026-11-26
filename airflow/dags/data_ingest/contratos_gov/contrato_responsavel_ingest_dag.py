"""Ingestão semanal dos responsáveis (fiscais e gestores) dos contratos do escopo.

GET /api/contrato/{contrato_id}/responsaveis não pagina nem filtra por data: a
única forma de ingerir é a carga completa, com o recorte temporal feito depois,
na Silver, por data_inicio e data_fim. Os contratos vêm das raws de ativos e
inativos, filtrados pelo órgão consumidor (Variable contratos_gov_escopo_orgaos);
se essas raws ainda não existem, ids_contratos_no_escopo falha dizendo qual DAG
rodar antes. Uma chamada por contrato é inevitável; expandir uma task por ID
excederia o core.max_map_length, então o Airflow expande apenas blocos
(ADR-0021).

Dado pessoal: usuario traz CPF mascarado e nome completo do servidor designado.
A raw guarda o campo como veio, porque mascarar na ingestão apagaria o dado sem
que ninguém decidisse isso. A proteção fica onde o repositório a aplica: o
catálogo classifica a entidade como pessoal (ADR-0013), e a validação de
publicação reprova bundle que exponha a coluna a consumidor de nível menor
(ADR-0020). Por isso esta DAG não loga registro nenhum, só contagens.

Cada linha é a designação de uma pessoa para uma função no contrato (funcao_id,
com portaria e período de atuação), e a chave é só id, como o catálogo declara.
A amostra de 2026-09-15 teve de 0 a 4 responsáveis por contrato: contrato sem
responsável conta como contrato vazio e não interrompe o bloco.

O payload já traz contrato_id; o valor da URL deve coincidir com ele em toda
linha, e a divergência falha antes da escrita, porque apontaria o responsável
(e os dados pessoais dele) para outro contrato. ClienteContratosGov.listar_subrecurso
confere (e preenche quando falta); _validar_lote repete a conferência como
segunda barreira, junto da checagem de id. Responsável sem id ou com id repetido
no mesmo lote interrompe a execução em vez de ser descartado: o ON CONFLICT do
warehouse recusa a mesma chave duas vezes no mesmo comando, e descartar
esconderia o caso.

Os nove campos do payload são planos, então write_raw é chamado sem
json_fields. A API recusou a conexão em 2026-11-25 e 2026-11-26, então o formato
saiu do código-fonte público do Comprasnet Contratos (gitlab.com/comprasnet/
contratos, master em adb9f9ee, Contratoresponsavel::responsavelAPI): contrato_id
inteiro; funcao_id e instalacao_id trazem a descrição em texto, não o id, apesar
do nome (instalação ausente vem como texto vazio); situacao vem como
'Ativo'/'Inativo'; não há e-mail nem telefone. No object_storage, cada contrato
tem arquivo próprio, para que as escritas do mesmo run não se sobrescrevam.

A varredura completa leva milhares de chamadas e horas de execução, e não deve
rodar na máquina do desenvolvedor: INGEST_MAX_CONTRATOS (definida só no
local.env) limita a amostra no compose, e a primeira execução completa ocorre
em homologação, depois do teste de carga da feature. Para validar localmente, a
raw de ativos e inativos precisa conter contratos de um órgão da Variable de
escopo (docs/notas/contratos-gov-ingestao.md).

A agenda é sábado 16:00: a janela dos sub-recursos começa às 12:00 e escalona
de 30 em 30 minutos na ordem das fases da issue #15; os responsáveis vêm meia
hora depois dos arquivos e meia hora antes dos prepostos.
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
ENTIDADE = "contrato_responsavel"
RECURSO = "responsaveis"
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
    repetir a chave primária. As mensagens citam só ids, nunca dado pessoal."""
    vistos: set[str] = set()
    for registro in registros:
        if str(registro.get("contrato_id")) != contrato_id:
            raise RuntimeError(
                f"Contrato {contrato_id}: responsável com contrato_id divergente da "
                f"URL ({registro.get('contrato_id')!r})."
            )
        responsavel_id = registro.get("id")
        if responsavel_id is None:
            raise RuntimeError(f"Contrato {contrato_id}: responsável sem id.")
        if str(responsavel_id) in vistos:
            raise RuntimeError(
                f"Contrato {contrato_id}: id de responsável {responsavel_id!r} "
                "repetido no lote."
            )
        vistos.add(str(responsavel_id))


@dag(
    dag_id="contrato_responsavel_ingest_dag",
    schedule="0 16 * * 6",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere os responsáveis (fiscais e gestores) por contrato da API "
        "Contratos.gov.br para contratos_gov.raw_contrato_responsavel, em blocos "
        "e no escopo dos órgãos consumidores. Contém dado pessoal."
    ),
    tags=["sistema:contratos_gov", "dominio:contratacoes"],
)
def contrato_responsavel_dag() -> None:
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
                # 200 [] é resposta legítima para contrato sem responsável.
                vazios += 1
                logging.info("Contrato %s: nenhum responsável.", contrato_id)
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
                "Contrato %s: %s responsáveis gravados.", contrato_id, len(registros)
            )

        return {"responsaveis": total, "contratos_vazios": vazios}

    @task
    def validate(results: Any) -> int:
        total = sum(resultado["responsaveis"] for resultado in results)
        vazios = sum(resultado["contratos_vazios"] for resultado in results)
        logging.info(
            "Responsáveis ingeridos: %s em %s blocos; contratos sem responsável: %s.",
            total,
            len(results),
            vazios,
        )
        return total

    blocos = get_contract_blocks()
    validate(ingest_contracts.expand(contrato_ids=blocos))


contrato_responsavel_dag()
