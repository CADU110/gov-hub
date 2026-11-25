from datetime import datetime, timedelta
import logging
import time
from airflow.sdk import dag, task, Param, get_current_context
from batching import chunked, limit_local
from cliente_compras_gov import ClienteComprasGov
from landing_zone import distinct_raw_values, write_raw

SISTEMA = "compras_gov"
BLOCK_SIZE = 50

default_args = {
    "owner": "mgi",
    "queue": "mgi",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="pgc_detalhe_ingest_dag",
    schedule="0 3 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere dados detalhados do módulo PGC Detalhe da API Compras.gov.br "
        "para a zona raw."
    ),
    tags=[
        "sistema:compras_gov",
        "orgao:mgi",
        "camada:raw",
        "dominio:pgc",
    ],
    params={
        "modo": Param(
            "incremental",
            enum=["incremental", "fria"],
            description="Modo de execução do DAG: incremental ou fria",
        ),
        "ano_inicio": Param(
            2023,
            type="integer",
            description="Ano inicial para consulta de PGC Detalhe (2023)",
        ),
    },
)
def pgc_detalhe_dag() -> None:

    @task
    def get_codigos_orgao() -> list[list[str]]:
        try:
            codigos = distinct_raw_values(
                SISTEMA,
                "orgao",
                "cnpjCpfOrgao",
            )
        except Exception as exc:
            raise RuntimeError(
                "Execute orgao_ingest_dag antes de pgc_detalhe_ingest_dag."
            ) from exc

        if not codigos:
            raise RuntimeError("Nenhum CNPJ de órgão foi encontrado na entidade 'orgao'.")

        codigos = limit_local(codigos, "INGEST_MAX_ORGAOS", "órgãos")
        blocos = chunked(codigos, BLOCK_SIZE)
        logging.info(
            "PGC Detalhe: %s órgãos em %s blocos de até %s para processar",
            len(codigos),
            len(blocos),
            BLOCK_SIZE,
        )

        return blocos

    @task(max_active_tis_per_dag=4)
    def fetch_pgc_detalhe(bloco: list[str]) -> dict:
        context = get_current_context()
        current_date = datetime.now()
        ano_atual = current_date.year
        modo = context["params"]["modo"]

        if modo == "fria":
            ano_inicio = context["params"]["ano_inicio"]
            ano_fim = ano_atual
            anos = list(range(ano_inicio, ano_fim + 1))
            logging.info("Modo de execução: fria. Anos a processar: %s", anos)
        else:
            anos = [ano_atual]
            logging.info("Modo de execução: incremental. Ano a processar: %s", anos)

        api = ClienteComprasGov()
        total_registros = 0

        for orgao in bloco:
            for ano in anos:
                try:
                    time.sleep(3)
                    registros = 0
                    # None distingue falha sem resposta de uma consulta válida vazia.
                    api_total = None
                    for batch, api_total in api.iter_pages(
                        "/modulo-pgc/1_consultarPgcDetalhe",
                        {"orgao": orgao, "anoPcaProjetoCompra": ano},
                    ):
                        write_raw(SISTEMA, "pgc_detalhe", batch)
                        registros += len(batch)

                    if api_total is None:
                        raise RuntimeError(
                            f"A API não retornou resposta válida: órgão={orgao}, ano={ano}."
                        )
                    if registros != api_total:
                        raise RuntimeError(
                            f"Resposta incompleta: órgão={orgao}, ano={ano}, "
                            f"gravados={registros}, api_total={api_total}."
                        )
                    total_registros += registros
                    logging.info(
                        "PGC Detalhe: órgão=%s, ano=%s, registros=%s",
                        orgao,
                        ano,
                        registros,
                    )

                except Exception:
                    logging.exception(
                        "Erro ao buscar PGC Detalhe para órgão=%s, ano=%s",
                        orgao,
                        ano,
                    )
                    raise
        return {"orgaos_processados": len(bloco), "registros": total_registros}

    @task
    def validate(results: list[dict]) -> None:
        total_orgaos = sum(r["orgaos_processados"] for r in results)
        total_registros = sum(r["registros"] for r in results)

        logging.info(
            "PGC Detalhe: órgãos=%s, total registros=%s",
            total_orgaos,
            total_registros,
        )

    blocos = get_codigos_orgao()
    resultados = fetch_pgc_detalhe.expand(bloco=blocos)

    # O Airflow resolve o XComArg para os resultados antes de executar a task.
    validate(resultados)  # ty: ignore[invalid-argument-type]


pgc_detalhe_dag()
