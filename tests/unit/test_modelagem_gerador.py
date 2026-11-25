"""Testes do gerador de modelos e do mapa de cruzamento (ADR-0017)."""

from __future__ import annotations

import textwrap
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from scripts.modelagem import gerador, macros
from scripts.modelagem.catalogo import ErroCatalogo, carregar
from scripts.modelagem.mapa import render, render_mermaid

CHAVES = """
versao: 1
chaves:
  co_orgao:
    nome: Código do órgão
    descricao: Código do órgão no Poder Executivo federal.
    normalizacao: digitos
    tamanho: 5
    sistema_de_referencia: siorg
    classificacao: publico
  co_uasg:
    nome: Código da UASG
    descricao: Unidade que executa compras.
    normalizacao: digitos
    tamanho: 6
    sistema_de_referencia: compras
    classificacao: publico
  co_ug:
    nome: Unidade gestora
    descricao: Unidade gestora que executa orçamento.
    normalizacao: digitos
    tamanho: 6
    sistema_de_referencia: orcamento
    classificacao: publico
  nu_matricula:
    nome: Matrícula
    descricao: Identificador do servidor.
    normalizacao: texto
    sistema_de_referencia: pessoal
    classificacao: pessoal
pontes:
  - de: co_uasg
    para: co_ug
    tipo: equivalencia
    confiabilidade: parcial
    descricao: UASG e unidade gestora coincidem na maioria dos casos.
"""

COMPRAS = """
sistema: compras
nome: Sistema de Compras
descricao: Compras do governo.
orgao_gestor: xpto
owner: "@GovHub-br/oss"
status: ingerido
entidades:
  contratos:
    descricao: Contratos administrativos.
    granularidade: uma linha por contrato
    dominio: contratacoes
    dag: contratos_ingest_dag
    origem_atual: bruto.raw_contratos
    chave_primaria: [nu_contrato]
    classificacao: publico
    chaves:
      - chave: co_uasg
        coluna: cd_unidade
        verificado: true
  unidades:
    descricao: Unidades administrativas.
    granularidade: uma linha por unidade
    dominio: organizacional
    origem_atual: bruto.raw_unidades
    chave_primaria: [cd_unidade]
    classificacao: publico
    chaves:
      - chave: co_uasg
        coluna: cd_unidade
        verificado: true
      - chave: co_orgao
        coluna: cd_orgao
        verificado: false
  isolada:
    descricao: Entidade sem chave conformada.
    granularidade: uma linha por registro
    dominio: avulso
    origem_atual: bruto.raw_isolada
    chave_primaria: [id]
    classificacao: publico
    chaves: []
"""

ORCAMENTO = """
sistema: orcamento
nome: Sistema de Orçamento
descricao: Execução orçamentária.
orgao_gestor: xpto
owner: "@GovHub-br/financas"
status: ingerido
entidades:
  empenhos:
    descricao: Empenhos emitidos.
    granularidade: uma linha por empenho
    dominio: execucao
    origem_atual: bruto.raw_empenhos
    chave_primaria: [nu_empenho]
    classificacao: publico
    chaves:
      - chave: co_ug
        coluna: cd_ug
        verificado: true
"""


@pytest.fixture
def catalogo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    base = tmp_path / "catalogo"
    (base / "sistemas").mkdir(parents=True)
    (base / "chaves.yml").write_text(textwrap.dedent(CHAVES), encoding="utf-8")
    for nome, conteudo in (("compras", COMPRAS), ("orcamento", ORCAMENTO)):
        (base / "sistemas" / f"{nome}.yml").write_text(
            textwrap.dedent(conteudo), encoding="utf-8"
        )
    # O gerador escreve relativo a DIR_DBT; isolamos a escrita no tmp_path.
    monkeypatch.setattr(gerador, "DIR_DBT", tmp_path / "dbt")
    return carregar(base)


def _yaml_do(caminho: Path) -> dict:
    return yaml.safe_load(caminho.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# Bronze
# --------------------------------------------------------------------------


def test_sources_declaram_a_bronze_com_identifier_da_origem(catalogo) -> None:
    (escrita,) = gerador.gerar_sources(catalogo, "compras")
    dados = _yaml_do(escrita.caminho)

    (fonte,) = dados["sources"]
    assert fonte["name"] == "compras"
    assert fonte["schema"] == "bruto"
    tabelas = {t["name"]: t for t in fonte["tables"]}
    assert tabelas["contratos"]["identifier"] == "raw_contratos"
    assert tabelas["contratos"]["meta"]["dag_ingestao"] == "contratos_ingest_dag"
    assert fonte["meta"]["camada"] == "bronze"


def test_sources_separam_bronze_materializada_sem_mudar_demais_origens(catalogo):
    sistema = catalogo.sistema("compras")
    sistema.entidades["contratos"] = replace(
        sistema.entidades["contratos"], origem_atual="001_bnz_compras.contratos"
    )
    (escrita,) = gerador.gerar_sources(catalogo, "compras")
    fontes = {s["name"]: s for s in _yaml_do(escrita.caminho)["sources"]}
    assert fontes["compras"]["schema"] == "bruto"
    assert {t["identifier"] for t in fontes["compras"]["tables"]} == {
        "raw_unidades",
        "raw_isolada",
    }
    bronze = fontes["compras_bronze"]
    assert bronze["schema"] == "001_bnz_compras"
    assert bronze["tables"][0]["identifier"] == "contratos"
    assert "target.type == 'duckdb'" in bronze["database"]
    assert "data_warehouse" in bronze["database"]
    sql, _ = gerador.gerar_silver(catalogo, "compras", "contratos")
    assert '{{ source("compras_bronze", "contratos") }}' in sql.caminho.read_text()


def test_sources_aceitam_sistema_inteiro_na_bronze_materializada(catalogo):
    sistema = catalogo.sistema("compras")
    for nome, entidade in sistema.entidades.items():
        sistema.entidades[nome] = replace(
            entidade, origem_atual=f"001_bnz_compras.{nome}"
        )
    (escrita,) = gerador.gerar_sources(catalogo, "compras")
    (fonte,) = _yaml_do(escrita.caminho)["sources"]
    assert fonte["name"] == "compras_bronze"
    assert len(fonte["tables"]) == 3


def test_sources_continuam_rejeitando_schemas_raw_ambiguos(catalogo):
    sistema = catalogo.sistema("compras")
    sistema.entidades["contratos"] = replace(
        sistema.entidades["contratos"], origem_atual="outro.raw_contratos"
    )
    with pytest.raises(ErroCatalogo, match="schemas diferentes"):
        gerador.gerar_sources(catalogo, "compras")


# --------------------------------------------------------------------------
# Silver
# --------------------------------------------------------------------------


def test_silver_normaliza_a_chave_conformada_e_deduplica(catalogo) -> None:
    sql, _ = gerador.gerar_silver(catalogo, "compras", "contratos")
    conteudo = sql.caminho.read_text(encoding="utf-8")

    assert sql.caminho.parts[-2] == "contratacoes"
    assert (
        '{{ gov_bricks.chave_conformada("co_uasg", "cd_unidade") }} as co_uasg'
        in conteudo
    )
    assert '{{ source("compras", "contratos") }}' in conteudo
    assert "partition by nu_contrato" in conteudo
    assert "where nu_versao = 1" in conteudo


def test_silver_documenta_metadados_obrigatorios_do_adr_0013(catalogo) -> None:
    _, doc = gerador.gerar_silver(catalogo, "compras", "contratos")
    modelo = _yaml_do(doc.caminho)["models"][0]

    assert modelo["name"] == "contratos"
    assert modelo["meta"]["owner"] == "@GovHub-br/oss"
    assert modelo["meta"]["sistema_origem"] == "compras"
    assert modelo["meta"]["classificacao"] == "publico"
    assert modelo["description"].strip()
    assert all(coluna["description"].strip() for coluna in modelo["columns"])


def test_classificacao_do_modelo_herda_a_coluna_mais_restritiva(catalogo) -> None:
    arquivo = gerador.caminho_pacote  # apenas para manter o import explícito
    assert callable(arquivo)

    _, doc = gerador.gerar_silver(catalogo, "compras", "unidades")
    modelo = _yaml_do(doc.caminho)["models"][0]
    # Todas as colunas são públicas, então o modelo é público.
    assert modelo["meta"]["classificacao"] == "publico"


def test_chave_nao_verificada_nao_ganha_teste_not_null(catalogo) -> None:
    _, doc = gerador.gerar_silver(catalogo, "compras", "unidades")
    colunas = {c["name"]: c for c in _yaml_do(doc.caminho)["models"][0]["columns"]}

    assert colunas["co_uasg"]["data_tests"] == ["not_null"]
    assert "data_tests" not in colunas["co_orgao"]


def test_dois_modelos_convivem_no_mesmo_schema_yml(catalogo) -> None:
    gerador.gerar_silver(catalogo, "compras", "contratos")
    _, doc = gerador.gerar_silver(catalogo, "compras", "isolada")
    # Domínios diferentes: cada um tem seu schema.yml.
    assert doc.caminho.parts[-2] == "avulso"

    gerador.gerar_silver(catalogo, "orcamento", "empenhos")
    nomes = [m["name"] for m in _yaml_do(doc.caminho)["models"]]
    assert nomes == ["isolada"]


def test_nao_sobrescreve_sem_forcar(catalogo) -> None:
    sql, _ = gerador.gerar_silver(catalogo, "compras", "contratos")
    sql.caminho.write_text("-- editado à mão\n", encoding="utf-8")

    repetido, _ = gerador.gerar_silver(catalogo, "compras", "contratos")
    assert repetido.acao == "preservado"
    assert sql.caminho.read_text(encoding="utf-8") == "-- editado à mão\n"

    forcado, _ = gerador.gerar_silver(catalogo, "compras", "contratos", forcar=True)
    assert forcado.acao == "atualizado"
    assert "chave_conformada" in sql.caminho.read_text(encoding="utf-8")


def test_entidade_inexistente_lista_as_catalogadas(catalogo) -> None:
    with pytest.raises(ErroCatalogo, match="contratos"):
        gerador.gerar_silver(catalogo, "compras", "nao_existe")


# --------------------------------------------------------------------------
# Gold — o cruzamento
# --------------------------------------------------------------------------


def test_cruzamento_direto_usa_a_chave_compartilhada(catalogo) -> None:
    passos = gerador.planejar_cruzamento(
        catalogo, ["compras.contratos", "compras.unidades"]
    )

    (passo,) = passos
    assert passo.chave_esquerda == "co_uasg"
    assert passo.chave_direita == "co_uasg"
    assert passo.ponte is None


def test_cruzamento_entre_sistemas_usa_a_ponte_declarada(catalogo) -> None:
    passos = gerador.planejar_cruzamento(
        catalogo, ["compras.contratos", "orcamento.empenhos"]
    )

    (passo,) = passos
    assert passo.ponte is not None
    assert (passo.chave_esquerda, passo.chave_direita) == ("co_uasg", "co_ug")
    assert passo.ponte.confiabilidade == "parcial"


def test_alias_e_qualificado_quando_ha_mais_de_um_sistema(catalogo) -> None:
    passos = gerador.planejar_cruzamento(
        catalogo, ["compras.contratos", "orcamento.empenhos"]
    )
    assert passos[0].alias == "orcamento_empenhos"
    assert passos[0].esquerda_alias == "compras_contratos"


def test_chave_transitiva_entra_no_conjunto_disponivel(catalogo) -> None:
    """orcamento.empenhos liga em compras.contratos via unidades."""
    passos = gerador.planejar_cruzamento(
        catalogo, ["compras.contratos", "compras.unidades", "orcamento.empenhos"]
    )
    assert [p.entidade.id for p in passos] == ["unidades", "empenhos"]


def test_entidade_sem_ligacao_falha_em_vez_de_gerar_produto_cartesiano(catalogo) -> None:
    with pytest.raises(ErroCatalogo, match="não se liga"):
        gerador.planejar_cruzamento(catalogo, ["compras.contratos", "compras.isolada"])


def test_cruzamento_exige_ao_menos_duas_entidades(catalogo) -> None:
    with pytest.raises(ErroCatalogo, match="duas entidades"):
        gerador.planejar_cruzamento(catalogo, ["compras.contratos"])


def test_referencia_mal_formada_e_recusada(catalogo) -> None:
    with pytest.raises(ErroCatalogo, match="sistema.entidade"):
        gerador.planejar_cruzamento(catalogo, ["contratos", "compras.unidades"])


def test_gold_gera_sql_com_join_e_aviso_de_ponte(catalogo) -> None:
    sql, doc = gerador.gerar_gold(
        catalogo,
        orgao="xpto",
        produto="execucao",
        nome="contratos_com_empenho",
        referencias=["compras.contratos", "orcamento.empenhos"],
    )
    conteudo = sql.caminho.read_text(encoding="utf-8")

    assert 'ref("compras", "contratos")' in conteudo
    assert 'ref("orcamento", "empenhos")' in conteudo
    assert (
        "left join orcamento_empenhos "
        "on compras_contratos.co_uasg = orcamento_empenhos.co_ug" in conteudo
    )
    assert "confiabilidade: parcial" in conteudo
    assert sql.caminho.parts[-3:] == ("gold", "execucao", "contratos_com_empenho.sql")

    modelo = _yaml_do(doc.caminho)["models"][0]
    assert modelo["meta"]["camada"] == "gold"
    assert modelo["meta"]["sistema_origem"] == "compras, orcamento"


# --------------------------------------------------------------------------
# Macros gerados e mapa
# --------------------------------------------------------------------------


def test_macro_gerado_reflete_o_catalogo(catalogo, tmp_path: Path) -> None:
    destino = tmp_path / "chaves_geradas.sql"
    assert macros.esta_sincronizado(catalogo, destino) is False

    macros.sincronizar(catalogo, destino)
    conteudo = destino.read_text(encoding="utf-8")

    assert macros.esta_sincronizado(catalogo, destino) is True
    assert '"co_orgao": {"normalizacao": "digitos", "tamanho": 5' in conteudo
    assert '"nu_matricula": {"normalizacao": "texto", "tamanho": none' in conteudo
    assert "{% macro chaves_conformadas() %}" in conteudo


def test_macro_do_repositorio_esta_sincronizado_com_o_catalogo() -> None:
    assert macros.esta_sincronizado(carregar()), "rode: make catalogo-sync"


def test_mapa_marca_verificado_e_nao_verificado(catalogo) -> None:
    texto = render(catalogo)

    assert "[ok] compras.contratos.cd_unidade" in texto
    assert "[??] compras.unidades.cd_orgao" in texto
    assert "compras.contratos ⨝ compras.unidades" in texto


def test_mapa_destaca_cruzamento_entre_sistemas(catalogo) -> None:
    linhas = [linha for linha in render(catalogo).splitlines() if "⨝" in linha]
    assert linhas, "nenhum cruzamento identificado"
    # O primeiro cruzamento listado é o de maior valor: entre sistemas distintos.
    assert "entre sistemas" in linhas[0]
    assert "compras.contratos ⨝ orcamento.empenhos" in linhas[0]


def test_mapa_revela_cruzamento_que_so_existe_por_ponte(catalogo) -> None:
    """contratos e empenhos não compartilham chave — ligam-se via co_uasg → co_ug."""
    linhas = [linha for linha in render(catalogo).splitlines() if "via ponte" in linha]

    assert any(
        "compras.contratos ⨝ orcamento.empenhos" in linha
        and "co_uasg → co_ug" in linha
        and "confiabilidade: parcial" in linha
        for linha in linhas
    )


def test_mapa_mermaid_liga_entidades_pela_chave(catalogo) -> None:
    grafo = render_mermaid(catalogo)
    assert grafo.startswith("graph LR")
    assert "|co_uasg|" in grafo
