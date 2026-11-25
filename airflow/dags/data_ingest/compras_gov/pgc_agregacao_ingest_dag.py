from datetime import datetime, timedelta
import logging
import time
from airflow.sdk import dag, task, Param, get_current_context
from batching import chunked
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
    dag_id="pgc_agregacao_ingest_dag",
    schedule="0 3 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    description=(
        "Ingere dados detalhados do módulo PGC Agregação da API Compras.gov.br "
        "para a zona raw."
    ),
    tags=["sistema:compras_gov", "orgao:mgi", "camada:raw", "dominio:pgc"],
    params={
        "modo": Param(
            "incremental",
            enum=["incremental", "fria"],
            description="Modo de execução do DAG: incremental ou fria",
        ),
        "ano_inicio": Param(
            2023,
            type="integer",
            description="Ano de início para a execução do dag PGC Agregação, (2023)",
        ),
    },
)
def pgc_agregacao_dag() -> None:

    @task
    def get_codigos_pgc() -> list[list[str]]:
        try:
            codigos = distinct_raw_values(
                SISTEMA,
                "orgao",
                "cnpjCpfOrgao",
            )
        except Exception as exc:
            raise RuntimeError(
                "Execute orgao_ingest_dag antes de pgc_agregacao_ingest_dag."
            ) from exc

        if not codigos:
            raise RuntimeError("Nenhum CNPJ de órgão foi encontrado na entidade 'orgao'.")

        blocos = chunked(codigos, BLOCK_SIZE)
        logging.info(
            "PGC Agregação: %s órgãos em %s blocos de até %s para processar",
            len(codigos),
            len(blocos),
            BLOCK_SIZE,
        )

        return blocos

    @task(max_active_tis_per_dag=1)
    def fetch_agregacao_pgc(bloco: list[str]) -> dict:
        context = get_current_context()
        current_year = datetime.now().year
        ano_atual = current_year
        modo = context["params"]["modo"]

        if modo == "fria":
            ano_inicio = context["params"]["ano_inicio"]
            ano_fim = ano_atual
            anos = range(ano_inicio, ano_fim + 1)
            logging.info("Modo de execução: fria. Anos a processar: %s", list(anos))
        else:
            anos = [ano_atual]
            logging.info("Modo de execução: incremental. Ano a processar: %s", anos)

        api = ClienteComprasGov()
        total_registros = 0

        for codigo_orgao in bloco:
            for ano in anos:
                try:
                    time.sleep(3)
                    registros = 0
                    # None distingue falha sem resposta de uma consulta válida vazia.
                    api_total = None
                    for batch, api_total in api.iter_pages(
                        "/modulo-pgc/3_consultarPgcAgregacao",
                        {"orgao": codigo_orgao, "ano": ano},
                    ):
                        write_raw(SISTEMA, "pgc_agregacao", batch)
                        registros += len(batch)

                    if api_total is None:
                        raise RuntimeError(
                            f"A API não retornou resposta válida: órgão={codigo_orgao}, ano={ano}."
                        )
                    if registros != api_total:
                        raise RuntimeError(
                            f"Resposta incompleta: órgão={codigo_orgao}, ano={ano}, "
                            f"gravados={registros}, api_total={api_total}."
                        )
                    total_registros += registros
                    logging.info(
                        "PGC Agregação: órgão=%s, ano=%s, registros=%s",
                        codigo_orgao,
                        ano,
                        registros,
                    )
                except Exception:
                    logging.exception(
                        "Erro ao buscar PGC Agregação: órgão=%s, ano=%s",
                        codigo_orgao,
                        ano,
                    )
                    raise

        return {
            "orgaos_processados": len(bloco),
            "registros": total_registros,
        }

    @task
    def validate(results: list[dict]) -> None:
        total_registros = sum(result["registros"] for result in results)
        total_orgaos = sum(result["orgaos_processados"] for result in results)

        logging.info(
            "PGC Agregação: órgãos processados=%s, registros=%s",
            total_orgaos,
            total_registros,
        )

    blocos = get_codigos_pgc()
    results = fetch_agregacao_pgc.expand(bloco=blocos)
    # O Airflow resolve o XComArg para os resultados antes de executar a task.
    validate(results)  # ty: ignore[invalid-argument-type]


pgc_agregacao_dag()
