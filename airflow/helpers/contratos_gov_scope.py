"""Seleciona os contratos detalhados pelas DAGs de sub-recursos.

O cabeçalho é varrido por UG, mas detalhar todos os contratos multiplicaria
cada contrato por todos os sub-recursos. A Variable limita esse custo aos
órgãos consumidores; ler ``id`` junto de ``orgao_codigo`` permite aplicar o
recorte antes de criar as chamadas à API (docs/notas/contratos-gov-ingestao.md).
"""

import re

from airflow.models import Variable

from landing_zone import RawIndisponivel, distinct_raw_rows

SISTEMA = "contratos_gov"
ENTIDADES = ("contrato_ativo", "contrato_inativo")
VARIAVEL_ESCOPO = "contratos_gov_escopo_orgaos"
# O Airflow devolve default_var sem desserializar; o default precisa já ser lista.
ESCOPO_PADRAO = ["46000"]
CODIGO_ORGAO = re.compile(r"^[0-9]{5}$")
COLUNAS_CONTRATO = ["id", "orgao_codigo"]


def orgaos_no_escopo() -> set[str]:
    """Lê e valida o recorte antes de disparar chamadas por contrato."""
    orgaos = Variable.get(
        VARIAVEL_ESCOPO, default_var=ESCOPO_PADRAO, deserialize_json=True
    )
    if (
        not isinstance(orgaos, list)
        or not orgaos
        or any(
            not isinstance(codigo, str) or not CODIGO_ORGAO.fullmatch(codigo)
            for codigo in orgaos
        )
    ):
        raise ValueError(
            f"Variable {VARIAVEL_ESCOPO} deve ser uma lista JSON não vazia "
            'de códigos SIAFI com cinco dígitos, por exemplo ["46000"].'
        )
    return set(orgaos)


def ids_contratos_no_escopo(orgaos: set[str]) -> list[str]:
    """Une ativos e inativos pela chave global, falhando se faltar uma raw."""
    ids: set[str] = set()
    for entidade in ENTIDADES:
        try:
            linhas = distinct_raw_rows(SISTEMA, entidade, COLUNAS_CONTRATO)
        except RawIndisponivel as exc:
            raise RuntimeError(
                f"Entidade '{entidade}' ainda não está na zona raw. "
                f"Execute {entidade}_ingest_dag antes das DAGs de detalhamento."
            ) from exc

        for contrato_id, orgao_codigo in linhas:
            if str(orgao_codigo) not in orgaos:
                continue
            if isinstance(contrato_id, bool) or not str(contrato_id).isdigit():
                raise RuntimeError(
                    f"A raw de {entidade} contém id de contrato inválido "
                    f"para o órgão {orgao_codigo}: {contrato_id!r}."
                )
            ids.add(str(contrato_id))

    if not ids:
        raise RuntimeError(
            f"Nenhum contrato dos órgãos {sorted(orgaos)!r} foi encontrado "
            "nas raws de contrato_ativo e contrato_inativo."
        )
    return sorted(ids, key=int)
