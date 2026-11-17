"""Ingestão semanal das ocorrências dos contratos no escopo de detalhamento.

GET /api/contrato/{contrato_id}/ocorrencias não pagina nem filtra por data: a
única forma de ingerir é a carga completa, com o recorte temporal feito depois,
na Silver, pelo campo data. Os contratos vêm das raws de ativos e inativos,
filtrados pelo órgão consumidor (Variable contratos_gov_escopo_orgaos). Uma
chamada por contrato é inevitável; expandir uma task por ID excederia o
core.max_map_length, então o Airflow expande apenas blocos (ADR-0021).

Entidade da fase 4: a API devolveu 200 [] em todos os contratos consultados,
então a estrutura não foi vista em dado real. Ela vem do código-fonte público
(Contratoocorrencia::ocorrenciaAPI) e da spec OpenAPI — ver o comentário da
entidade em catalogo/sistemas/contratos_gov.yml. Duas consequências:

- 200 [] é a resposta esperada para a maioria dos contratos, não anomalia.
  Se nenhum contrato do escopo tiver ocorrência, nada é gravado e, no backend
  warehouse, raw_contrato_ocorrencia só passa a existir na primeira execução
  que encontrar uma; validate registra isso no log em vez de falhar.
- A chave é só id (chave da tabela contratoocorrencias na origem, sem tabela de
  vínculo). id repetido no lote com conteúdo idêntico é descartado com aviso,
  porque o ON CONFLICT do warehouse recusa a mesma chave duas vezes no mesmo
  comando; com conteúdo diferente, falha, porque não há como escolher a certa.

O payload traz contrato_id. ClienteContratosGov.listar_subrecurso confere esse
valor contra o da URL em vez de sobrescrevê-lo; _validar_lote repete a
conferência como segunda barreira, junto da checagem de id, e falha antes da
escrita se a ocorrência apontar para outro contrato.

arquivos é lista de objetos {"arquivo_N": url}: vai como JSON no warehouse e
como objeto nativo no object_storage. usuario (nome completo e CPF parcial) e
emailpreposto são dado pessoal — a entidade é classificada como pessoal no
catálogo. No object_storage, cada contrato tem arquivo próprio, para que as
escritas do mesmo run não se sobrescrevam.

A varredura completa leva milhares de chamadas e horas de execução, e NÃO deve
rodar na máquina do desenvolvedor: INGEST_MAX_CONTRATOS (definida só no
local.env) limita a amostra no compose, e a primeira execução completa ocorre
em homologação, depois do teste de carga da feature. Com o limite padrão de 10
UGs no compose, os cabeçalhos ingeridos podem não conter contratos do MGI
(órgão 46000); para validar localmente, a raw de ativos e inativos precisa
conter contratos de um órgão da Variable de escopo
(docs/notas/contratos-gov-ingestao.md).
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
ENTIDADE = "contrato_ocorrencia"
RECURSO = "ocorrencias"
PK = ["id"]
JSON_FIELDS = ["arquivos"]
BLOCK_SIZE = 25
MAX_BLOCOS = 1024

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


def _validar_lote(contrato_id: str, registros: list[dict]) -> list[dict]:
    """Devolve o lote sem cópias idênticas, falhando antes da escrita se ele
    apontar para outro contrato, vier sem id ou repetir id com conteúdo
    diferente."""
    unicos: dict[str, dict] = {}
    for registro in registros:
        if str(registro.get("contrato_id")) != contrato_id:
            raise RuntimeError(
                f"Contrato {contrato_id}: ocorrência com contrato_id divergente "
                f"da URL ({registro.get('contrato_id')!r})."
            )
        ocorrencia_id = registro.get("id")
        if ocorrencia_id is None:
            raise RuntimeError(f"Contrato {contrato_id}: ocorrência sem id.")
        if unicos.setdefault(str(ocorrencia_id), registro) != registro:
            raise RuntimeError(
                f"Contrato {contrato_id}: id de ocorrência {ocorrencia_id!r} "
                "repetido no lote com conteúdo diferente."
            )
    return list(unicos.values())


@dag(
    dag_id="contrato_ocorrencia_ingest_dag",
    schedule="0 18 * * 6",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere ocorrências por contrato da API Contratos.gov.br para "
        "contratos_gov.raw_contrato_ocorrencia, em blocos e no escopo dos órgãos "
        "consumidores."
    ),
    tags=["sistema:contratos_gov", "dominio:contratacoes"],
)
def contrato_ocorrencia_dag() -> None:
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
            # Fora de uma DagRun, write_raw gera o identificador do arquivo.
            run_id = None

        total = 0
        vazios = 0
        repetidas = 0
        for contrato_id in contrato_ids:
            registros = api.listar_subrecurso(contrato_id, RECURSO)
            if not registros:
                # 200 [] é a resposta comum: contrato sem ocorrência registrada.
                vazios += 1
                logging.info("Contrato %s: nenhuma ocorrência.", contrato_id)
                continue

            lote = _validar_lote(contrato_id, registros)
            descartadas = len(registros) - len(lote)
            if descartadas:
                repetidas += descartadas
                logging.warning(
                    "Contrato %s: %s ocorrência(s) idêntica(s) repetida(s) "
                    "na resposta, descartada(s) antes da escrita.",
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
            logging.info(
                "Contrato %s: %s ocorrência(s) gravada(s).", contrato_id, len(lote)
            )

        return {
            "ocorrencias": total,
            "contratos_vazios": vazios,
            "linhas_repetidas": repetidas,
        }

    @task
    def validate(results: Any) -> int:
        total = sum(resultado["ocorrencias"] for resultado in results)
        vazios = sum(resultado["contratos_vazios"] for resultado in results)
        repetidas = sum(resultado["linhas_repetidas"] for resultado in results)
        logging.info(
            "Ocorrências ingeridas: %s em %s blocos; contratos sem ocorrência: %s; "
            "linhas repetidas descartadas: %s.",
            total,
            len(results),
            vazios,
            repetidas,
        )
        if total == 0:
            logging.warning(
                "Nenhum dos %s contrato(s) consultados tem ocorrência: nada foi "
                "gravado nesta execução. Se nenhuma execução anterior gravou, "
                "%s.raw_%s ainda não existe no backend warehouse (nem há Parquet "
                "no object_storage) e a Bronze não tem o que ler. Esperado na "
                "fase 4 enquanto a fonte devolver apenas listas vazias.",
                vazios,
                SISTEMA,
                ENTIDADE,
            )
        return total

    blocos = get_contract_blocks()
    validate(ingest_contracts.expand(contrato_ids=blocos))


contrato_ocorrencia_dag()
