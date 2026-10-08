"""
DAG de ingestão dos cabeçalhos de contrato inativo do Contratos.gov.br.

Endpoint: GET /api/contrato/inativo/ug/{unidade_codigo}
Fonte: https://contratos.comprasnet.gov.br (API aberta, sem autenticação)

Mesmo desenho da contrato_ativo_ingest_dag (issue #18): o endpoint não pagina,
não filtra por período e só existe por unidade gestora. A lista de UGs vem da
raw que a unidade_contratante_ingest_dag grava, particionada em blocos para não
estourar o core.max_map_length (airflow/helpers/batching.py), com uma escrita
por UG para que falha no meio do bloco não descarte o que já veio.

O que muda em relação aos ativos: UG sem contrato inativo é a regra.
    Na amostra de 2026-10-07 (12 UGs sorteadas), 8 voltaram vazias e as 4
    restantes somaram 100 contratos. Por isso a execução sem nenhum contrato
    só é tratada como anomalia da fonte quando a varredura cobriu pelo menos
    MIN_UGS_PARA_EXIGIR_CONTRATO UGs. Com dois terços de UGs vazias, 50 UGs
    todas vazias por acaso tem probabilidade da ordem de 1e-9; já as 10 UGs do
    INGEST_MAX_UGS local voltariam todas vazias em cerca de 2% das execuções,
    e a DAG falharia no compose sem nada de errado na fonte.

UG que tinha contrato inativo na raw e voltou vazia vira alerta, não erro.
    Contrato inativo raramente deixa de sê-lo, então a queda para zero é sinal
    mais forte aqui do que nos ativos; ainda assim reativação é possível na
    fonte, e quem decide é quem lê o alerta.

Por que só as UGs de unidade_contratante?
    A lista de /api/contrato/unidades se comporta como lista de UGs com contrato
    ativo: numa amostra de 60 UGs dela (2026-10-07), nenhuma estava sem ativo.
    UG que só tem contrato inativo fica, portanto, fora desta varredura. A
    primeira versão aceita essa lacuna de propósito: alcançá-la pede outra
    lista de UGs (a proposta da issue #19 é compras_gov.uasg), que é dependência
    entre sistemas e multiplica as chamadas por UGs que na maioria não têm
    contrato nenhum. Fica registrada em docs/notas/contratos-gov-ingestao.md.

Por que não roda inteira na máquina do desenvolvedor?
    A varredura completa são milhares de chamadas, estimadas em horas. No
    compose, INGEST_MAX_UGS (local.env) trunca a lista e é com essa amostra que
    a DAG é validada; a primeira execução completa acontece em homologação.

Chave primária: id (inteiro, global no sistema, o mesmo id do contrato ativo).
    No backend warehouse, write_raw faz upsert por `id`; no object_storage a
    raw é append-only e a Silver deduplica por dt_ingest. Um contrato que é
    inativado entre duas varreduras passa a existir nas duas raws — em
    raw_contrato_ativo com a foto antiga, em raw_contrato_inativo com a nova.
    A Silver que unifica as duas precisa deduplicar por `id` entre elas pelo
    dt_ingest, não só dentro de cada uma.

Horário: sábado às 10:00, duas horas depois da varredura de ativos (08:00), que
    leva cerca de 1h10 com concorrência 4. Assim as duas varreduras não disputam
    a API e ficam fora da janela do compras_gov e da transformação do MGI
    (docs/notas/contratos-gov-ingestao.md).
"""

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from batching import chunked, limit_local
from cliente_contratos_gov import ClienteContratosGov
from landing_zone import RawIndisponivel, distinct_raw_values, write_raw

SISTEMA = "contratos_gov"
ENTIDADE = "contrato_inativo"
DEPENDENCIA = "unidade_contratante"
PK = ["id"]
COLUNA_UG = "unidade_gestora_codigo"
BLOCK_SIZE = 25
MIN_UGS_PARA_EXIGIR_CONTRATO = 50

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="contrato_inativo_ingest_dag",
    schedule="0 10 * * 6",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere o cabeçalho dos contratos inativos por unidade gestora da API "
        "Contratos.gov.br (GET /api/contrato/inativo/ug/{codigo}) para "
        "contratos_gov.raw_contrato_inativo. Varredura completa semanal em blocos de UGs."
    ),
    tags=["sistema:contratos_gov", "dominio:contratacoes"],
)
def contrato_inativo_dag() -> None:
    @task
    def get_ug_blocks() -> list[list[str]]:
        try:
            ugs = distinct_raw_values(SISTEMA, DEPENDENCIA, "codigo")
        except Exception as exc:
            raise RuntimeError(
                f"Entidade '{DEPENDENCIA}' ainda não está na zona raw. "
                "Execute unidade_contratante_ingest_dag antes de "
                "contrato_inativo_ingest_dag."
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

    @task
    def get_ugs_com_contrato() -> list[str]:
        """UGs que já têm contrato inativo na raw, lidas antes da varredura."""
        try:
            ugs = distinct_raw_values(SISTEMA, ENTIDADE, COLUNA_UG)
        except RawIndisponivel:
            logging.info(
                "Entidade '%s' ainda não está na raw: primeira execução, "
                "sem base para comparar UGs que zeraram.",
                ENTIDADE,
            )
            return []
        logging.info("UGs com contrato inativo antes desta execução: %s", len(ugs))
        return ugs

    @task(max_active_tis_per_dag=4)
    def ingest_ugs(codigos_ug: list[str]) -> dict:
        api = ClienteContratosGov()
        contratos = 0
        ugs_vazias = []

        for codigo_ug in codigos_ug:
            registros = api.listar_contratos_inativos_ug(codigo_ug)

            # UG inexistente e UG sem contrato inativo respondem igual (200 []),
            # e aqui isso é o caso comum. A UG vazia volta nomeada para o
            # validate cruzar com as que já tinham contrato na raw.
            if not registros:
                logging.info("UG %s: nenhum contrato inativo.", codigo_ug)
                ugs_vazias.append(codigo_ug)
                continue

            write_raw(SISTEMA, ENTIDADE, registros, primary_key=PK)
            logging.info("UG %s: contratos=%s", codigo_ug, len(registros))
            contratos += len(registros)

        return {"contratos": contratos, "ugs_vazias": ugs_vazias}

    @task
    def validate(results: Any, ugs_com_contrato: Any = None) -> int:
        total = sum(r["contratos"] for r in results)
        vazias = [ug for r in results for ug in r["ugs_vazias"]]
        zeradas = sorted(set(vazias) & set(ugs_com_contrato or []))
        logging.info(
            "Contratos inativos total: contratos=%s blocos=%s ugs_vazias=%s",
            total,
            len(results),
            len(vazias),
        )

        if zeradas:
            logging.warning(
                "ALERTA: %s UG(s) tinham contrato inativo na raw e voltaram "
                "vazias nesta execução: %s",
                len(zeradas),
                ", ".join(zeradas),
            )

        if total == 0:
            if len(vazias) < MIN_UGS_PARA_EXIGIR_CONTRATO:
                logging.warning(
                    "Nenhum contrato inativo nas %s UGs varridas. Abaixo de %s "
                    "UGs isso é plausível (a maioria das UGs não tem inativo), "
                    "então a execução segue.",
                    len(vazias),
                    MIN_UGS_PARA_EXIGIR_CONTRATO,
                )
                return total
            raise RuntimeError(
                "Execução sem nenhum contrato inativo em nenhuma das "
                f"{len(vazias)} UGs varridas. A fonte devolver tudo vazio é "
                "anomalia, não realidade: interrompendo antes de a Silver ler "
                "uma raw incompleta."
            )

        return total

    ugs_antes = get_ugs_com_contrato()
    blocos = get_ug_blocks()
    resultados = ingest_ugs.expand(codigos_ug=blocos)
    ugs_antes >> resultados
    validate(resultados, ugs_antes)


contrato_inativo_dag()
