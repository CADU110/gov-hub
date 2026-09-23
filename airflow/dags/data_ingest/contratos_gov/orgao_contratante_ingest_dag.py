"""Ingestão diária dos órgãos contratantes expostos pelo Contratos.gov.br.

O endpoint não oferece paginação nem filtro temporal, portanto cada execução
faz uma única carga completa. A lista contém somente órgãos com contrato ativo
e ``codigo`` precisa permanecer texto para preservar zeros à esquerda. Uma
resposta vazia é tratada como anomalia da fonte e falha antes da escrita; aceitar
o lote vazio poderia aparentar, incorretamente, que todos os órgãos sumiram.
Código duplicado também falha antes da escrita: no backend ``warehouse`` o
``ON CONFLICT DO UPDATE`` recusa a mesma chave duas vezes no mesmo comando, e em
``object_storage`` as duas linhas teriam o mesmo ``dt_ingest``, deixando a
deduplicação da Silver sem critério para escolher uma.
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from cliente_contratos_gov import ClienteContratosGov
from landing_zone import write_raw

SISTEMA = "contratos_gov"
ENTIDADE = "orgao_contratante"
ENDPOINT = "/api/contrato/orgaos"
PK = ["codigo"]
CODIGO_ORGAO = re.compile(r"^[0-9]{5}$")

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _validar_orgaos(registros: list[dict[str, Any]]) -> None:
    if not registros:
        raise RuntimeError(
            f"[{ENDPOINT}] Resposta vazia: eram esperados centenas de órgãos. "
            "O lote não será gravado."
        )

    codigos: set[str] = set()
    duplicados: set[str] = set()
    for indice, registro in enumerate(registros):
        codigo = registro.get("codigo")
        if not isinstance(codigo, str) or not CODIGO_ORGAO.fullmatch(codigo):
            raise RuntimeError(
                f"[{ENDPOINT}] Registro {indice} possui codigo inválido: {codigo!r}. "
                "Era esperada uma string de cinco dígitos."
            )
        if codigo in codigos:
            duplicados.add(codigo)
        codigos.add(codigo)

    if duplicados:
        raise RuntimeError(
            f"[{ENDPOINT}] Códigos duplicados na resposta: {sorted(duplicados)!r}. "
            "O lote não será gravado."
        )


@dag(
    dag_id="orgao_contratante_ingest_dag",
    schedule="10 22 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere os órgãos com contrato ativo da API Contratos.gov.br para "
        "contratos_gov.raw_orgao_contratante."
    ),
    tags=["sistema:contratos_gov", "dominio:contratacoes"],
)
def orgao_contratante_dag() -> None:
    @task
    def fetch_and_store() -> dict[str, int]:
        registros = ClienteContratosGov().listar_orgaos()
        _validar_orgaos(registros)
        write_raw(SISTEMA, ENTIDADE, registros, primary_key=PK)
        logging.info("[%s] Lote completo gravado: %s órgão(s).", ENDPOINT, len(registros))
        return {"ingeridos": len(registros)}

    fetch_and_store()


orgao_contratante_dag()
