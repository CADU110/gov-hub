"""Exercita o fluxo Parquet → Bronze → Silver inicial de cada domínio DuckDB.

Roda os SQLs reais (a Bronze via `gov_bricks.bronze_parquet`, a Silver do
domínio e os testes genéricos `mesma_contagem` e `mesmo_conteudo_em_snake_case`)
num DuckDB local, com o object storage emulado em disco, sem serviços externos.

Cada Bronze em Parquet precisa de uma amostra em AMOSTRAS: é o único dado por
domínio; o resto do teste vale para qualquer um.
"""

import os
import re
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import TypedDict

import duckdb
import pytest
import yaml
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.implementations.local import LocalFileSystem
from jinja2 import Environment, StrictUndefined

RAIZ = Path(__file__).resolve().parents[2]
DBT = RAIZ / "airflow/dags/dbt"
SISTEMA = "compras_gov"
PACOTE = DBT / SISTEMA
MACROS = DBT / "gov_bricks/macros"

CLASSIFICACOES = {"publico", "interno", "pessoal", "pessoal_sensivel"}


class Amostra(TypedDict):
    colunas: list[str]
    tipos: dict[str, str]


# Contrato observado nos Parquets de cada entidade: colunas na ordem em que
# chegam da raw e os tipos que não são texto, para detectar conversão acidental.
AMOSTRAS: dict[str, Amostra] = {
    "arp": {
        "colunas": [
            "numeroAtaRegistroPreco",
            "codigoUnidadeGerenciadora",
            "nomeUnidadeGerenciadora",
            "codigoOrgao",
            "nomeOrgao",
            "linkAtaPNCP",
            "linkCompraPNCP",
            "numeroCompra",
            "anoCompra",
            "codigoModalidadeCompra",
            "nomeModalidadeCompra",
            "dataAssinatura",
            "dataVigenciaInicial",
            "dataVigenciaFinal",
            "valorTotal",
            "statusAta",
            "objeto",
            "quantidadeItens",
            "dataHoraAtualizacao",
            "dataHoraInclusao",
            "dataHoraExclusao",
            "ataExcluido",
            "numeroControlePncpAta",
            "numeroControlePncpCompra",
            "idCompra",
            "dt_ingest",
        ],
        "tipos": {
            "codigoOrgao": "BIGINT",
            "quantidadeItens": "BIGINT",
            "valorTotal": "DOUBLE",
            "ataExcluido": "BOOLEAN",
        },
    },
}

VALORES = {"BIGINT": 48000, "DOUBLE": 12.5, "BOOLEAN": True, "VARCHAR": "  original  "}

BRONZES = sorted(p.stem for p in (PACOTE / "models/bronze").glob("*.sql"))


class _Retorno(Exception):
    """Equivalente ao MacroReturn do dbt: carrega o valor do `return()`."""

    def __init__(self, valor):
        self.valor = valor


def _retornar(valor):
    raise _Retorno(valor)


def _chamar(macro, *args, **kwargs):
    try:
        return macro(*args, **kwargs)
    except _Retorno as retorno:
        return retorno.valor


def _modulo(texto, **contexto):
    return (
        Environment(undefined=StrictUndefined)
        .from_string(texto)
        .make_module({"return": _retornar, **contexto})
    )


def _gov_bricks(conexao, entidade):
    """O namespace `gov_bricks` que o dbt injeta, com os macros reais.

    `{% test %}` é sintaxe do dbt; aqui ele vira `{% macro test_<nome> %}`,
    que é o que o próprio dbt faz ao carregar um teste genérico.
    """
    gov_bricks = SimpleNamespace()
    testes = re.sub(
        r"{%\s*test\s+(\w+)\(", r"{% macro test_\1(", (MACROS / "testes.sql").read_text()
    )
    testes = re.sub(r"{%\s*endtest\s*%}", "{% endmacro %}", testes)
    adapter = SimpleNamespace(
        quote=lambda nome: f'"{nome}"',
        get_columns_in_relation=lambda relacao: [
            SimpleNamespace(name=linha[0])
            for linha in conexao.sql(f"describe {relacao}").fetchall()
        ],
    )
    modulo_testes = _modulo(
        testes,
        adapter=adapter,
        execute=True,
        exceptions=SimpleNamespace(raise_compiler_error=pytest.fail),
        modules=SimpleNamespace(re=re),
        gov_bricks=gov_bricks,
    )
    modulo_bronze = _modulo(
        (MACROS / "bronze_parquet.sql").read_text(),
        env_var=os.getenv,
        model=SimpleNamespace(package_name=SISTEMA, name=entidade),
    )
    gov_bricks.snake_case = lambda nome: _chamar(modulo_testes.snake_case, nome)
    gov_bricks.bronze_parquet = modulo_bronze.bronze_parquet
    gov_bricks.teste = lambda nome, **kw: str(
        getattr(modulo_testes, f"test_{nome}")(**kw)
    )
    return gov_bricks


def _renderizar(caminho, gov_bricks):
    return (
        Environment(undefined=StrictUndefined)
        .from_string(caminho.read_text())
        .render(gov_bricks=gov_bricks, ref=lambda nome: f'"{nome}"')
    )


def _silver_sobre(entidade):
    """A Silver de um domínio DuckDB que lê a Bronze da entidade."""
    (silver,) = [
        p
        for p in (PACOTE / "models/silver").glob("*/*.sql")
        if f'ref("{entidade}")' in p.read_text()
    ]
    return silver


def test_toda_bronze_parquet_tem_amostra():
    assert set(BRONZES) == set(AMOSTRAS)


@pytest.mark.parametrize("bucket", [None, "outro-bucket"])
@pytest.mark.parametrize("entidade", BRONZES)
def test_bronze_e_silver_preservam_linhas_valores_e_tipos(
    tmp_path, monkeypatch, entidade, bucket
):
    if bucket is None:
        monkeypatch.delenv("MINIO_BUCKET", raising=False)
    else:
        monkeypatch.setenv("MINIO_BUCKET", bucket)
    colunas = AMOSTRAS[entidade]["colunas"]
    tipos = {c: AMOSTRAS[entidade]["tipos"].get(c, "VARCHAR") for c in colunas}

    class S3Local(DirFileSystem):
        protocol = "s3"

    with duckdb.connect() as conexao:
        conexao.register_filesystem(S3Local(path=str(tmp_path), fs=LocalFileSystem()))
        gov_bricks = _gov_bricks(conexao, entidade)

        declaracoes = ", ".join(f'"{c}" {tipos[c]}' for c in colunas)
        conexao.execute(f"create table amostra ({declaracoes})")
        linha = [VALORES[tipos[c]] for c in colunas]
        conexao.executemany(
            f"insert into amostra values ({', '.join('?' for _ in colunas)})",
            [linha, linha, [None] * len(colunas)],
        )
        pasta = tmp_path / (bucket or "data-lake") / SISTEMA / entidade / "2026/10"
        pasta.mkdir(parents=True)
        conexao.execute("copy amostra to ? (format parquet)", [str(pasta / "p.parquet")])

        sql_bronze = _renderizar(PACOTE / "models/bronze" / f"{entidade}.sql", gov_bricks)
        assert f"s3://{bucket or 'data-lake'}/{SISTEMA}/{entidade}/" in sql_bronze
        conexao.execute(f'create table "{entidade}" as {sql_bronze}')

        silver = _silver_sobre(entidade)
        sql_silver = _renderizar(silver, gov_bricks)
        for _ in range(2):
            conexao.execute(f'create or replace table "{silver.stem}" as {sql_silver}')
            assert Counter(conexao.sql(f'select * from "{silver.stem}"').fetchall()) == (
                Counter(conexao.sql(f'select * from "{entidade}"').fetchall())
            )

        # Mesmo tipo em cada coluna, pelo nome em snake_case.
        descricao = conexao.sql(f'describe "{silver.stem}"').fetchall()
        tipos_silver = {nome: tipo for nome, tipo, *_ in descricao}
        assert tipos_silver == {gov_bricks.snake_case(c): tipos[c] for c in colunas}

        # schema.yml documenta exatamente as colunas da Silver, na ordem.
        (metadados,) = yaml.safe_load(silver.with_name("schema.yml").read_text())[
            "models"
        ]
        assert [c["name"] for c in metadados["columns"]] == list(tipos_silver)
        assert all(
            c["description"] and c["meta"]["classificacao"] in CLASSIFICACOES
            for c in metadados["columns"]
        )

        # Os testes genéricos passam e detectam divergência.
        def teste(nome):
            sql = gov_bricks.teste(
                nome, model=f'"{silver.stem}"', comparar_com=f'"{entidade}"'
            )
            return conexao.sql(sql).fetchall()

        assert teste("mesma_contagem") == []
        assert teste("mesmo_conteudo_em_snake_case") == []
        texto = gov_bricks.snake_case(next(c for c in colunas if tipos[c] == "VARCHAR"))
        conexao.execute(
            f"update \"{silver.stem}\" set {texto} = 'alterado' where {texto} is not null"
        )
        assert teste("mesma_contagem") == []
        assert len(teste("mesmo_conteudo_em_snake_case")) == 4
        conexao.execute(f'delete from "{silver.stem}" where {texto} is null')
        assert len(teste("mesma_contagem")) == 1


@pytest.mark.parametrize("entidade", BRONZES)
def test_bronze_materializada_e_declarada_no_catalogo_e_no_source(entidade):
    """A Bronze em Parquet substitui a tabela raw como origem da entidade."""
    catalogo = yaml.safe_load((RAIZ / f"catalogo/sistemas/{SISTEMA}.yml").read_text())
    assert catalogo["entidades"][entidade]["origem_atual"] == (
        f"001_bnz_{SISTEMA}.{entidade}"
    )
    fontes = {
        f["name"]: f
        for f in yaml.safe_load((PACOTE / "models/bronze/sources.yml").read_text())[
            "sources"
        ]
    }
    bronze = fontes[f"{SISTEMA}_bronze"]
    assert bronze["schema"] == f"001_bnz_{SISTEMA}"
    assert entidade in {t["identifier"] for t in bronze["tables"]}
    assert entidade not in {t["name"] for t in fontes[SISTEMA]["tables"]}
    assert f'ref("{entidade}")' in _silver_sobre(entidade).read_text()


@pytest.mark.parametrize(
    "nome,esperado",
    [
        ("numeroAtaRegistroPreco", "numero_ata_registro_preco"),
        ("linkAtaPNCP", "link_ata_pncp"),
        ("numeroControlePncpAta", "numero_controle_pncp_ata"),
        ("CNPJFornecedor", "cnpj_fornecedor"),
        ("valor2Etapa", "valor2_etapa"),
        ("objeto", "objeto"),
        ("dt_ingest", "dt_ingest"),
    ],
)
def test_snake_case_trata_siglas_como_uma_palavra(nome, esperado):
    with duckdb.connect() as conexao:
        assert _gov_bricks(conexao, "x").snake_case(nome) == esperado
