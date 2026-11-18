"""Ingestão semanal do domicílio bancário dos contratos no escopo de detalhamento.

GET /api/contrato/{contrato_id}/domiciliobancario não pagina nem filtra por data:
a única forma de ingerir é consultar contrato a contrato, com o recorte temporal
feito depois, na Silver, por data_registro e updated_at de cada operação. Os
contratos vêm das raws de ativos e inativos, filtrados pelo órgão consumidor
(Variable contratos_gov_escopo_orgaos). Expandir uma task por ID excederia o
core.max_map_length, então o Airflow expande apenas blocos (batching.py).

O endpoint foge das convenções dos demais sub-recursos, e a DAG não copia a de
ocorrências nesse ponto:

- a resposta de sucesso é UM objeto por contrato, não lista:
  {unidade_gestora, contrato_id, contrato, domiciliosbancarios: [...]}. A raw
  guarda esse objeto como veio, uma linha por contrato, com domiciliosbancarios
  em JSON no warehouse e como lista nativa no object_storage. Nada é achatado
  nem explodido aqui; a Silver explode a lista;
- a chave é contrato_id. Cada id da lista é o de uma operação AntecipaGov, que a
  fonte pode ligar a vários contratos (tabela contrato_antecipagov), então id
  não identifica a linha. O upsert por contrato_id troca o objeto inteiro a
  cada execução, e uma operação removida na fonte não sobra no warehouse;
- a ausência é 404, não 200 []. "Contrato sem domicílio bancário..." é a
  resposta comum e conta como vazio; "Contrato não localizado." (contrato
  apagado na fonte depois da varredura de cabeçalhos, que no object_storage
  continua na raw append-only) é contado à parte e avisado, sem derrubar o
  bloco. Qualquer outro 404, 5xx ou timeout falha a task
  (ClienteContratosGov.consultar_domicilio_bancario).

Entidade da fase 4: em 2026-11-17, todos os 40 contratos consultados
responderam "sem domicílio", então a estrutura não foi vista em dado real; ela
vem da OpenAPI e do código-fonte público (comentário da entidade em
catalogo/sistemas/contratos_gov.yml). Se nenhum contrato do escopo tiver
domicílio, nada é gravado, e no backend warehouse
raw_contrato_domicilio_bancario só passa a existir na primeira execução que
encontrar um; validate registra isso no log em vez de falhar.

O cliente confere o contrato_id do objeto contra o da URL; _validar_domicilio
repete a conferência como segunda barreira e exige id em cada operação antes da
escrita. Operação repetida com conteúdo idêntico é mantida, porque a raw guarda
o objeto da fonte e o vínculo contrato_antecipagov aceita o mesmo par duas
vezes, mas é contada e avisada; o mesmo id com conteúdo diferente falha. Os
campos bancários e unidade_gestora chegam como texto e assim ficam: zero à
esquerda não se perde. A entidade é classificada como pessoal no catálogo
(conta de fornecedor pessoa física). No object_storage, cada contrato tem
arquivo próprio, para que as escritas do mesmo run não se sobrescrevam.

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
from cliente_contratos_gov import ClienteContratosGov, ContratoNaoLocalizado
from contratos_gov_scope import ids_contratos_no_escopo, orgaos_no_escopo
from landing_zone import write_raw

SISTEMA = "contratos_gov"
ENTIDADE = "contrato_domicilio_bancario"
PK = ["contrato_id"]
JSON_FIELDS = ["domiciliosbancarios"]
BLOCK_SIZE = 25
MAX_BLOCOS = 1024
# Quantos ids de contrato não localizado o validate cita no aviso.
MAX_IDS_NO_LOG = 20

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


def _validar_domicilio(contrato_id: str, domicilio: dict) -> int:
    """Confere o objeto antes da escrita, sem alterá-lo; devolve quantas
    operações idênticas repetidas ele traz.

    Falha se o objeto apontar para outro contrato, se domiciliosbancarios não
    for lista de objetos com id, ou se o mesmo id vier com conteúdo diferente.
    """
    if str(domicilio.get("contrato_id")) != contrato_id:
        raise RuntimeError(
            f"Contrato {contrato_id}: domicílio bancário com contrato_id divergente "
            f"da URL ({domicilio.get('contrato_id')!r})."
        )
    operacoes = domicilio.get("domiciliosbancarios")
    if not isinstance(operacoes, list):
        raise RuntimeError(
            f"Contrato {contrato_id}: domiciliosbancarios ausente ou fora do "
            "formato de lista."
        )

    vistas: dict[str, dict] = {}
    repetidas = 0
    for operacao in operacoes:
        if not isinstance(operacao, dict) or operacao.get("id") is None:
            raise RuntimeError(
                f"Contrato {contrato_id}: operação de domicílio bancário sem id."
            )
        chave = str(operacao["id"])
        if chave not in vistas:
            vistas[chave] = operacao
            continue
        if vistas[chave] != operacao:
            raise RuntimeError(
                f"Contrato {contrato_id}: id de operação {operacao['id']!r} "
                "repetido com conteúdo diferente."
            )
        repetidas += 1
    return repetidas


@dag(
    dag_id="contrato_domicilio_bancario_ingest_dag",
    schedule="30 18 * * 6",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere o domicílio bancário (AntecipaGov) por contrato da API "
        "Contratos.gov.br para contratos_gov.raw_contrato_domicilio_bancario, em "
        "blocos e no escopo dos órgãos consumidores."
    ),
    tags=["sistema:contratos_gov", "dominio:contratacoes"],
)
def contrato_domicilio_bancario_dag() -> None:
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
    def ingest_contracts(contrato_ids: list[str]) -> dict[str, Any]:
        api = ClienteContratosGov()
        try:
            run_id = str(get_current_context()["run_id"])
        except RuntimeError:
            # Fora de uma DagRun, write_raw gera o identificador do arquivo.
            run_id = None

        com_domicilio = 0
        operacoes = 0
        sem_domicilio = 0
        repetidas = 0
        nao_localizados: list[str] = []
        for contrato_id in contrato_ids:
            try:
                domicilio = api.consultar_domicilio_bancario(contrato_id)
            except ContratoNaoLocalizado:
                nao_localizados.append(contrato_id)
                logging.warning(
                    "Contrato %s: não localizado na fonte; nada a gravar.", contrato_id
                )
                continue
            if domicilio is None:
                # 404 "sem domicílio" é a resposta comum, não erro.
                sem_domicilio += 1
                continue

            repetidas_contrato = _validar_domicilio(contrato_id, domicilio)
            quantidade = len(domicilio["domiciliosbancarios"])
            if repetidas_contrato:
                repetidas += repetidas_contrato
                logging.warning(
                    "Contrato %s: %s operação(ões) idêntica(s) repetida(s) em "
                    "domiciliosbancarios, mantida(s) como a fonte entregou.",
                    contrato_id,
                    repetidas_contrato,
                )
            if not quantidade:
                logging.warning(
                    "Contrato %s: 200 com domiciliosbancarios vazio, que a fonte "
                    "documenta como 404; gravado como veio.",
                    contrato_id,
                )

            write_raw(
                SISTEMA,
                ENTIDADE,
                [domicilio],
                primary_key=PK,
                run_id=f"{run_id}__contrato_{contrato_id}" if run_id else None,
                json_fields=JSON_FIELDS,
            )
            com_domicilio += 1
            operacoes += quantidade
            logging.info(
                "Contrato %s: domicílio bancário gravado com %s operação(ões).",
                contrato_id,
                quantidade,
            )

        return {
            "contratos_com_domicilio": com_domicilio,
            "operacoes": operacoes,
            "contratos_sem_domicilio": sem_domicilio,
            "contratos_nao_localizados": nao_localizados,
            "operacoes_repetidas": repetidas,
        }

    @task
    def validate(results: Any) -> int:
        resultados = list(results)
        gravados = sum(r["contratos_com_domicilio"] for r in resultados)
        operacoes = sum(r["operacoes"] for r in resultados)
        sem_domicilio = sum(r["contratos_sem_domicilio"] for r in resultados)
        repetidas = sum(r["operacoes_repetidas"] for r in resultados)
        nao_localizados = [
            contrato_id
            for r in resultados
            for contrato_id in r["contratos_nao_localizados"]
        ]
        consultados = gravados + sem_domicilio + len(nao_localizados)
        logging.info(
            "Domicílios bancários: %s contrato(s) gravado(s) com %s operação(ões) em "
            "%s blocos; sem domicílio: %s; não localizados: %s; operações "
            "repetidas mantidas: %s.",
            gravados,
            operacoes,
            len(resultados),
            sem_domicilio,
            len(nao_localizados),
            repetidas,
        )
        if nao_localizados:
            if len(nao_localizados) == consultados:
                raise RuntimeError(
                    f"Nenhum dos {consultados} contrato(s) consultados foi localizado "
                    "na fonte: a raw de cabeçalhos e a API não falam dos mesmos "
                    "contratos. Confira o ambiente da API e a raw antes de repetir."
                )
            logging.warning(
                "%s contrato(s) da raw não localizado(s) na fonte (apagados depois "
                "da varredura de cabeçalhos?): %s%s",
                len(nao_localizados),
                ", ".join(nao_localizados[:MAX_IDS_NO_LOG]),
                " ..." if len(nao_localizados) > MAX_IDS_NO_LOG else "",
            )
        if gravados == 0:
            logging.warning(
                "Nenhum dos %s contrato(s) consultados tem domicílio bancário: nada "
                "foi gravado nesta execução. Se nenhuma execução anterior gravou, "
                "%s.raw_%s ainda não existe no backend warehouse (nem há Parquet "
                "no object_storage) e a Bronze não tem o que ler. Esperado na "
                "fase 4 enquanto a fonte só responder 404 sem domicílio.",
                consultados,
                SISTEMA,
                ENTIDADE,
            )
        return gravados

    blocos = get_contract_blocks()
    validate(ingest_contracts.expand(contrato_ids=blocos))


contrato_domicilio_bancario_dag()
