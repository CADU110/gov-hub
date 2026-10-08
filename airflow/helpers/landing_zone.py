import json
import logging
import os
from datetime import date, datetime
from typing import TYPE_CHECKING
import uuid


import polars as pl

from cliente_storage import ensure_bucket_exists, get_bucket, get_storage_fs

if TYPE_CHECKING:
    from airflow.sdk import Asset


def build_landing_path(
    source: str,
    entity: str,
    run_date: date,
    run_id: str,
    ext: str = "parquet",
) -> str:
    """Return the full bucket-qualified path for a landing zone file.

    Pattern: {bucket}/{source}/{entity}/{year}/{month}/{day}/{run_id}.{ext}
    """
    bucket = get_bucket()
    return (
        f"{bucket}/{source}/{entity}/"
        f"{run_date.year}/{run_date.month:02d}/{run_date.day:02d}/"
        f"{run_id}.{ext}"
    )


def write_parquet(df: pl.DataFrame, path: str) -> str:
    """Write a Polars DataFrame as Parquet to the landing zone. Returns path."""
    fs = get_storage_fs()
    ensure_bucket_exists(fs, get_bucket())
    with fs.open(path, "wb") as f:
        df.write_parquet(f)
    logging.info(f"[landing_zone] Wrote {len(df)} rows → {path}")
    return path


def read_parquet(path: str) -> pl.DataFrame:
    """Read a Parquet file from the landing zone into a Polars DataFrame."""
    fs = get_storage_fs()
    with fs.open(path, "rb") as f:
        df = pl.read_parquet(f)
    logging.info(f"[landing_zone] Read {len(df)} rows ← {path}")
    return df


def list_files(prefix: str, ext: str = "parquet") -> list[str]:
    """List all files under a landing zone prefix with the given extension."""
    fs = get_storage_fs()
    try:
        return fs.glob(f"{prefix}/**/*.{ext}")
    except FileNotFoundError:
        return []


# ---------------------------------------------------------------------------
# Zona raw — a porta única por onde a ingestão entrega o dado (ADR-0021).
# ---------------------------------------------------------------------------

BACKEND_OBJECT_STORAGE = "object_storage"
BACKEND_WAREHOUSE = "warehouse"
BACKENDS = (BACKEND_OBJECT_STORAGE, BACKEND_WAREHOUSE)

# Connection do destino analítico do órgão — não a do banco de apoio do Airflow.
CONEXAO_WAREHOUSE_PADRAO = "postgres_dw"

# Coluna que marca quando a linha foi ingerida. Carimbada aqui, e não em cada
# DAG, para que os dois backends gravem a mesma coisa.
COLUNA_INGESTAO = "dt_ingest"


def get_raw_backend() -> str:
    """Forma física da zona raw neste deployment (ADR-0021).

    O padrão é object storage, como decidiu o ADR-0012; um órgão que roda só um
    banco relacional configura `warehouse` e dispensa o object storage.
    """
    backend = os.getenv("RAW_BACKEND", BACKEND_OBJECT_STORAGE).strip().lower()
    if backend not in BACKENDS:
        raise ValueError(
            f"RAW_BACKEND '{backend}' desconhecido. Use um de: {', '.join(BACKENDS)}."
        )
    return backend


def raw_table_name(entity: str) -> str:
    """Nome da tabela raw de uma entidade, no backend `warehouse`."""
    return f"raw_{entity}"


def asset_raw(source: str, entity: str) -> "Asset":
    """Asset do Airflow que representa uma entidade na zona raw.

    A DAG de ingestão o declara como `outlet` da task que fecha a execução, e a
    DAG de transformação do domínio (ADR-0018) é agendada por ele: a Bronze só
    lê a raw depois que todos os lotes do dia foram gravados, em vez de depender
    de uma janela de horário. Montar o Asset aqui garante que os dois lados usem
    o mesmo nome. O nome não depende do backend, então trocar o RAW_BACKEND não
    quebra o agendamento.

    O import é local pelo mesmo motivo do `_run_id_do_contexto`: o resto do
    módulo funciona sem Airflow.
    """
    from airflow.sdk import Asset

    return Asset(f"raw.{source}.{entity}")


def stamp_ingestion(records: list[dict], moment: datetime | None = None) -> list[dict]:
    """Carimba a coluna de ingestão nos registros que ainda não a têm."""
    marca = (moment or datetime.now()).isoformat()
    for registro in records:
        registro.setdefault(COLUNA_INGESTAO, marca)
    return records


def write_raw(
    source: str,
    entity: str,
    records: list[dict],
    primary_key: list[str] | None = None,
    run_id: str | None = None,
    run_date: date | None = None,
    conn_id: str = CONEXAO_WAREHOUSE_PADRAO,
    json_fields: list[str] | None = None,
) -> str | None:
    """Entrega um lote à zona raw e devolve o destino onde ele ficou.

    É a única função que uma DAG de ingestão usa para gravar: ela não sabe, e
    não deve saber, se o destino é object storage ou um banco (ADR-0011,
    ADR-0021). Devolve `None` quando não há registro nenhum a gravar.
    """
    if not records:
        logging.warning("[landing_zone] Nada a gravar em %s.%s.", source, entity)
        return None

    carimbados = stamp_ingestion(records)
    backend = get_raw_backend()

    if backend == BACKEND_OBJECT_STORAGE:
        return _write_raw_object_storage(source, entity, carimbados, run_id, run_date)
    return _write_raw_warehouse(
        source, entity, carimbados, primary_key, conn_id, json_fields
    )


def _run_id_do_contexto(source: str, entity: str) -> str:
    """Descobre o run_id da execução em curso, quando a DAG não o informa.

    O run_id é o que torna cada arquivo rastreável até a execução que o produziu
    (ADR-0012). Buscá-lo aqui evita ter que passar o contexto do Airflow por
    toda task de ingestão; fora de uma execução — em teste, por exemplo — cai
    para um identificador de horário, com aviso.
    """
    try:
        from airflow.sdk import get_current_context

        return str(get_current_context()["run_id"])
    except Exception:
        alternativo = f"sem-run-id-{datetime.now():%Y%m%dT%H%M%S%f}"
        logging.warning(
            "[landing_zone] Sem contexto de execução ao gravar %s.%s; "
            "usando run_id '%s'.",
            source,
            entity,
            alternativo,
        )
        return alternativo


def _write_raw_object_storage(
    source: str,
    entity: str,
    records: list[dict],
    run_id: str | None,
    run_date: date | None,
) -> str:
    """Grava um Parquet novo, imutável, na convenção do ADR-0012."""
    run_id = run_id or _run_id_do_contexto(source, entity)
    identificador = f"{run_id}--lote-{uuid.uuid4().hex[:8]}"
    caminho = build_landing_path(source, entity, run_date or date.today(), identificador)
    # `infer_schema_length=None` faz o Polars olhar todos os registros antes de
    # decidir o tipo: com amostragem, uma coluna nula nas primeiras linhas viraria
    # Null e perderia os valores das seguintes.
    return write_parquet(pl.DataFrame(records, infer_schema_length=None), caminho)


def _write_raw_warehouse(
    source: str,
    entity: str,
    records: list[dict],
    primary_key: list[str] | None,
    conn_id: str,
    json_fields: list[str] | None = None,
) -> str:
    """Materializa o lote como tabela raw no destino analítico do órgão.

    O import é local de propósito: em `object_storage` o deployment pode não ter
    driver de banco nenhum instalado, e importar o cliente no topo do módulo
    quebraria a ingestão inteira por uma dependência que ele não usa.
    """
    from cliente_postgres import ClientPostgresDB
    from postgres_helpers import get_postgres_conn

    dados_sql = [
        {
            campo: (
                json.dumps(valor, ensure_ascii=False)
                if campo in (json_fields or []) and isinstance(valor, (dict, list))
                else valor
            )
            for campo, valor in registro.items()
        }
        for registro in records
    ]
    tabela = raw_table_name(entity)
    cliente = ClientPostgresDB(get_postgres_conn(conn_id))
    cliente.insert_data(
        dados_sql,
        tabela,
        primary_key=primary_key,
        conflict_fields=primary_key,
        schema=source,
    )
    destino = f"{source}.{tabela}"
    logging.info("[landing_zone] %s registro(s) → %s", len(records), destino)
    return destino


class RawIndisponivel(RuntimeError):
    """A entidade pedida ainda não foi ingerida na zona raw."""


def read_raw(source: str, entity: str, columns: list[str]) -> pl.DataFrame:
    """Lê colunas de uma entidade da zona raw, seja qual for o backend.

    Existe porque parte das DAGs de ingestão é dirigida pelo que já foi
    ingerido — contratos itera sobre os órgãos, pesquisa de preço sobre os itens
    de catálogo. Sem esta função, essas DAGs teriam que consultar o banco por
    SQL e continuariam presas a um motor específico (ADR-0011, ADR-0021).
    """
    if get_raw_backend() == BACKEND_OBJECT_STORAGE:
        return _read_raw_object_storage(source, entity, columns)
    return _read_raw_warehouse(source, entity, columns)


def distinct_raw_values(source: str, entity: str, column: str) -> list[str]:
    """Valores distintos e não nulos de uma coluna da raw, ordenados."""
    dados = read_raw(source, entity, [column])
    valores = {
        str(valor)
        for valor in dados[column].to_list()
        if valor is not None and valor != ""
    }
    return sorted(valores)


def distinct_raw_rows(source: str, entity: str, columns: list[str]) -> list[tuple]:
    """Combinações distintas de colunas da raw, ordenadas.

    É o equivalente agnóstico de um `SELECT DISTINCT ... ORDER BY`: parte das
    DAGs de ingestão pagina sobre essas combinações para saber o que buscar na
    fonte. A deduplicação acontece em memória, e não no motor — o preço de não
    depender de um.
    """
    dados = read_raw(source, entity, columns).unique().sort(columns)
    return [tuple(linha) for linha in dados.iter_rows()]


def _read_raw_object_storage(
    source: str, entity: str, columns: list[str]
) -> pl.DataFrame:
    """Lê todos os Parquets já gravados da entidade.

    A raw é imutável e append-only (ADR-0012): a visão atual de uma entidade é a
    união dos arquivos de todas as execuções, e a deduplicação é responsabilidade
    da Silver, não daqui.
    """
    prefixo = f"{get_bucket()}/{source}/{entity}"
    arquivos = list_files(prefixo)
    if not arquivos:
        raise RawIndisponivel(
            f"Nenhum arquivo na zona raw sob {prefixo} — a entidade '{entity}' "
            "ainda não foi ingerida."
        )
    return pl.concat(
        [read_parquet(arquivo).select(columns) for arquivo in arquivos], how="vertical"
    )


def _read_raw_warehouse(source: str, entity: str, columns: list[str]) -> pl.DataFrame:
    from cliente_postgres import ClientPostgresDB
    from postgres_helpers import get_postgres_conn

    tabela = f"{source}.{raw_table_name(entity)}"
    consulta = f"SELECT {', '.join(columns)} FROM {tabela}"
    try:
        linhas = ClientPostgresDB(
            get_postgres_conn(CONEXAO_WAREHOUSE_PADRAO)
        ).execute_query(consulta)
    except Exception as exc:
        raise RawIndisponivel(
            f"Não foi possível ler {tabela} — a entidade '{entity}' ainda não foi "
            "ingerida neste destino."
        ) from exc
    return pl.DataFrame(
        {coluna: [linha[i] for linha in linhas] for i, coluna in enumerate(columns)}
    )
