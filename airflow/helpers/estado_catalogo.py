import polars as pl

from landing_zone import COLUNA_INGESTAO


def contar_inativos(dados: pl.DataFrame, coluna_codigo: str, coluna_status: str) -> int:
    """Quantos códigos têm o registro mais recente da raw, por `dt_ingest`, inativo.

    Um código reativado ganha linha ativa nova na execução base e sai da conta; um
    código inativado só entra depois que a DAG de inativos o grava. No backend
    `warehouse` o upsert já deixa só a linha atual, e a conta dá o mesmo resultado.
    O status chega booleano no Parquet e como texto no `warehouse`, onde toda
    coluna raw é TEXT.
    """
    ultimos = dados.sort(COLUNA_INGESTAO).unique(subset=[coluna_codigo], keep="last")
    status = pl.col(coluna_status).cast(pl.String).str.to_lowercase()
    return ultimos.filter(status == "false").height
