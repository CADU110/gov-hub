"""Silver do Compras.gov no schema do domínio e ramo DuckDB separado por tag.

Cobre duas regressões: um `+schema` na pasta `silver` movia todas as Silvers
existentes para um schema só (ADR-0010), e os nós que leem Parquet entravam na
DAG que roda no PostgreSQL. Os testes percorrem os arquivos do pacote, então
valem para todo domínio novo sem mudança aqui.
"""

from pathlib import Path
from types import SimpleNamespace

import jinja2
import pytest
import yaml

DBT = Path(__file__).resolve().parents[2] / "airflow/dags/dbt"
PACOTE = DBT / "compras_gov"
SILVERS = sorted((PACOTE / "models/silver").glob("*/*.sql"))


class Retorno(Exception):
    def __init__(self, valor):
        self.valor = valor


def renderizar_macro(arquivo, nome, *args, **contexto):
    def retornar(valor):
        raise Retorno(valor)

    modulo = (
        jinja2.Environment(undefined=jinja2.StrictUndefined)
        .from_string(arquivo.read_text())
        .make_module({"return": retornar, **contexto})
    )
    with pytest.raises(Retorno) as resultado:
        getattr(modulo, nome)(*args)
    return resultado.value.valor


def schema_padrao(node):
    return renderizar_macro(
        DBT / "gov_bricks/macros/schema_medallion.sql", "schema_medallion", node
    )


def config_modelos():
    config = yaml.safe_load((PACOTE / "dbt_project.yml").read_text())
    return config["models"]["compras_gov"]


@pytest.mark.parametrize("arquivo", SILVERS, ids=lambda p: p.parent.name + "/" + p.stem)
def test_cada_silver_fica_no_schema_do_proprio_dominio(arquivo):
    dominio = arquivo.parent.name
    silver = config_modelos()["silver"]
    assert silver.get("+schema") is None
    assert silver.get(dominio, {}).get("+schema") is None
    node = SimpleNamespace(
        path=f"silver/{dominio}/{arquivo.name}", package_name="compras_gov"
    )
    assert (
        renderizar_macro(
            PACOTE / "macros/generate_schema_name.sql",
            "generate_schema_name",
            None,
            node,
            gov_bricks=SimpleNamespace(schema_medallion=schema_padrao),
        )
        == f"002_slv_{dominio}"
    )


def test_silvers_que_ja_existiam_continuam_fora_do_ramo_duckdb():
    silver = config_modelos()["silver"]
    for dominio in ("contratacoes", "fornecedores", "organizacional"):
        assert dominio not in silver


def test_bronze_toda_em_parquet_leva_a_tag_duckdb():
    """A pasta bronze/ só tem modelos que leem Parquet (a relacional é source)."""
    bronze = config_modelos()["bronze"]
    assert bronze["+tags"] == ["duckdb"]
    for modelo in (PACOTE / "models/bronze").glob("*.sql"):
        assert "gov_bricks.bronze_parquet()" in modelo.read_text(), modelo.name


@pytest.mark.parametrize("arquivo", SILVERS, ids=lambda p: p.parent.name + "/" + p.stem)
def test_silver_que_le_bronze_parquet_leva_a_tag_duckdb(arquivo):
    """Silver sobre Bronze em Parquet precisa rodar no DuckDB, e só ela."""
    bronzes = {p.stem for p in (PACOTE / "models/bronze").glob("*.sql")}
    le_parquet = any(f'ref("{b}")' in arquivo.read_text() for b in bronzes)
    tags = config_modelos()["silver"].get(arquivo.parent.name, {}).get("+tags", [])
    assert ("duckdb" in tags) == le_parquet


@pytest.mark.parametrize(
    "tipo,banco,esperado",
    [
        ("duckdb", "memory", "data_warehouse"),
        ("postgres", "warehouse_local", "warehouse_local"),
    ],
)
def test_database_e_o_mesmo_banco_fisico_nos_dois_targets(tipo, banco, esperado):
    """O database mantém o ref() possível entre os ramos PostgreSQL e DuckDB.

    No DuckDB, `data_warehouse` é o alias do PostgreSQL anexado; no PostgreSQL,
    o próprio banco do target. Os dois apontam para a mesma tabela.
    """
    assert (
        jinja2.Template(config_modelos()["+database"]).render(
            target=SimpleNamespace(type=tipo, database=banco)
        )
        == esperado
    )


def test_bronze_e_outros_pacotes_mantem_convencao():
    for caminho, pacote, esperado in [
        ("bronze/arp.sql", "compras_gov", "001_bnz_compras_gov"),
        ("silver/contratos/contrato.sql", "contratos_gov", "002_slv_contratos"),
        ("gold/contratacoes/contratos_por_orgao.sql", "mgi", "003_gld_contratacoes"),
    ]:
        assert (
            schema_padrao(SimpleNamespace(path=caminho, package_name=pacote)) == esperado
        )
    for pacote in ("mgi", "contratos_gov"):
        config = yaml.safe_load((DBT / pacote / "dbt_project.yml").read_text())
        assert config["models"][pacote].get("silver", {}).get("+schema") is None


@pytest.mark.parametrize("arquivo", SILVERS, ids=lambda p: p.parent.name + "/" + p.stem)
def test_silver_usa_nome_do_arquivo_sem_alias(arquivo):
    assert "alias=" not in arquivo.read_text()
    documentados = yaml.safe_load(arquivo.with_name("schema.yml").read_text())["models"]
    assert arquivo.stem in {m["name"] for m in documentados}
