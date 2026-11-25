"""Geração de modelos dbt a partir do catálogo (ADR-0009, ADR-0013, ADR-0017).

O gerador escreve três coisas:

- ``models/bronze/sources.yml`` — fontes relacionais declaradas no catálogo,
  incluindo tabelas raw e tabelas Bronze já materializadas;
- ``models/silver/<dominio>/<entidade>.sql`` — a Silver, com as chaves
  conformadas já normalizadas e a deduplicação por chave primária;
- ``models/gold/<produto>/<entidade>.sql`` — a Gold, com o cruzamento entre
  entidades já resolvido pelas chaves conformadas que elas compartilham.

Nada é sobrescrito sem ``forcar=True``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from .catalogo import (
    RAIZ,
    Catalogo,
    Chave,
    Entidade,
    ErroCatalogo,
    Mapeamento,
    Ponte,
    Sistema,
    classificacao_mais_restritiva,
)

DIR_DBT = RAIZ / "airflow" / "dags" / "dbt"

COLUNA_INGESTAO = "dt_ingest"

# Pacote onde vivem os macros compartilhados. A chamada precisa ser qualificada:
# um modelo de pacote (compras_gov) não enxerga, sem qualificar, o macro de outro
# pacote (gov_bricks) — dbt só procura no pacote do próprio modelo e na raiz.
PACOTE_BASE = "gov_bricks"


@dataclass(frozen=True)
class Escrita:
    """Um arquivo produzido (ou preservado) pelo gerador."""

    caminho: Path
    acao: str  # "criado", "atualizado" ou "preservado"

    def __str__(self) -> str:
        return f"{self.acao:<11} {self.caminho.relative_to(RAIZ)}"


@dataclass(frozen=True)
class Coluna:
    """Uma coluna documentada em schema.yml (ADR-0013)."""

    nome: str
    descricao: str
    classificacao: str
    # Só recebe teste not_null a chave conformada cujo mapeamento já foi
    # verificado contra o dado ingerido — testar uma coluna presumida faria o
    # build reprovar por uma hipótese, não por um defeito.
    testar: bool = False
    # Por que a coluna não recebeu not_null, quando for o caso — vai como
    # comentário no schema.yml para que a ausência do teste seja deliberada e
    # legível, não um esquecimento.
    motivo_sem_teste: str | None = None


def _decidir_teste(mapeamento: Mapeamento | None) -> tuple[bool, str | None]:
    """Se a coluna recebe not_null e, quando não recebe, por quê."""
    if mapeamento is None:
        return False, None
    if not mapeamento.verificado:
        return False, "mapeamento ainda não verificado contra o dado ingerido"
    if mapeamento.opcional:
        return False, "chave opcional nesta entidade — nula em parte das linhas"
    return True, None


@dataclass(frozen=True)
class Passo:
    """Um join do plano de cruzamento de uma Gold."""

    sistema: Sistema
    entidade: Entidade
    alias: str
    esquerda_alias: str
    chave_esquerda: str
    chave_direita: str
    ponte: Ponte | None = None


def _quebrar(texto: str, largura: int = 76, recuo: str = "      ") -> str:
    """Quebra um texto em linhas indentadas, para descrição de YAML."""
    palavras = texto.split()
    linhas: list[str] = []
    atual = ""
    for palavra in palavras:
        if atual and len(atual) + len(palavra) + 1 > largura:
            linhas.append(atual)
            atual = palavra
        else:
            atual = f"{atual} {palavra}".strip()
    if atual:
        linhas.append(atual)
    return "\n".join(f"{recuo}{linha}" for linha in linhas) or f"{recuo}-"


def _escrever(caminho: Path, conteudo: str, forcar: bool) -> Escrita:
    if caminho.exists() and not forcar:
        return Escrita(caminho, "preservado")
    acao = "atualizado" if caminho.exists() else "criado"
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_text(conteudo, encoding="utf-8")
    return Escrita(caminho, acao)


def caminho_pacote(id_sistema: str) -> Path:
    """Pasta do pacote dbt de um sistema estruturante (ADR-0004, ADR-0009)."""
    return DIR_DBT / id_sistema


def caminho_projeto(orgao: str) -> Path:
    """Pasta do projeto dbt de um órgão (ADR-0004, ADR-0009)."""
    return DIR_DBT / orgao


# --------------------------------------------------------------------------
# Bronze — declaração das fontes relacionais do catálogo (ADR-0017).
# --------------------------------------------------------------------------


def gerar_sources(
    catalogo: Catalogo, id_sistema: str, forcar: bool = False
) -> list[Escrita]:
    """Escreve models/bronze/sources.yml com todas as entidades do sistema."""
    sistema = catalogo.sistema(id_sistema)
    entidades = [e for e in sistema.entidades.values() if e.origem_atual]
    if not entidades:
        raise ErroCatalogo(
            f"Sistema '{id_sistema}' não tem entidade com 'origem_atual' — "
            "não há Bronze para declarar como source."
        )

    # A migração pode ocorrer entidade por entidade: preservar o source antigo
    # das tabelas raw e declarar a Bronze materializada em seu próprio schema.
    materializadas = [e for e in entidades if e.schema_origem == sistema.schema_bronze]
    raw = [e for e in entidades if e.schema_origem != sistema.schema_bronze]
    schemas = {e.schema_origem for e in raw}
    if len(schemas) > 1:
        raise ErroCatalogo(
            f"Entidades de '{id_sistema}' apontam para schemas diferentes "
            f"({', '.join(sorted(s or '?' for s in schemas))}); declare os sources à mão."
        )
    schema = (schemas.pop() if schemas else None) or sistema.schema_bronze

    linhas = [
        "# GERADO por `make modelo` a partir de catalogo/sistemas/"
        f"{sistema.id}.yml — não edite à mão.",
        "#",
        "# A Bronze não é materializada por dbt: ela é um source sobre a zona raw,",
        "# onde a ingestão entrega o dado (ADR-0006, ADR-0017, ADR-0021).",
        "#",
        "# Esta é a forma de tabela, do backend `warehouse`. Em um deployment com a",
        "# raw em object storage, o source aponta para os arquivos da entidade.",
        "",
        "version: 2",
        "",
        "sources:",
    ]
    if materializadas:
        linhas[2:7] = [
            "# Fontes relacionais descritas por origem_atual no catálogo.",
            "# Tabelas raw mantêm o source do sistema; a Bronze materializada",
            "# usa o source <sistema>_bronze, sem alterar a ingestão.",
        ]
    if raw:
        linhas += _linhas_source(sistema, raw, sistema.id, schema)
    if materializadas:
        linhas += _linhas_source(
            sistema, materializadas, f"{sistema.id}_bronze", sistema.schema_bronze
        )

    caminho = caminho_pacote(sistema.id) / "models" / "bronze" / "sources.yml"
    # sources.yml é integralmente derivado do catálogo: regerar sempre é seguro.
    return [_escrever(caminho, "\n".join(linhas).rstrip() + "\n", forcar=True)]


def _linhas_source(
    sistema: Sistema, entidades: list[Entidade], nome: str, schema: str
) -> list[str]:
    materializada = schema == sistema.schema_bronze
    origem = "pelo dbt" if materializada else "pelas DAGs de ingestão"
    linhas = [
        f"  - name: {nome}",
        f"    schema: {schema}",
    ]
    if materializada:
        # No DuckDB o nome é o alias do PostgreSQL anexado, não o arquivo local.
        linhas.append(
            "    database: \"{{ 'data_warehouse' if target.type == 'duckdb' "
            "else env_var('DBT_DATABASE', 'data_warehouse') }}\""
        )
    linhas += [
        "    description: >",
        _quebrar(
            f"Camada Bronze de {sistema.nome}: dado fiel à fonte, gravado "
            f"{origem}. {sistema.descricao}",
            recuo="      ",
        ),
        "    meta:",
        f'      owner: "{sistema.owner}"',
        f"      sistema_origem: {sistema.id}",
        "      camada: bronze",
        "    tables:",
    ]
    for entidade in sorted(entidades, key=lambda e: e.id):
        linhas += [
            f"      - name: {entidade.id}",
            f"        identifier: {entidade.tabela_origem}",
            "        description: >",
            _quebrar(
                f"{entidade.descricao} Granularidade: {entidade.granularidade}.",
                recuo="            ",
            ),
            "        meta:",
            f"          classificacao: {entidade.classificacao}",
        ]
        if entidade.dag:
            linhas.append(f"          dag_ingestao: {entidade.dag}")
        linhas.append("")

    return linhas


# --------------------------------------------------------------------------
# Silver
# --------------------------------------------------------------------------


def _colunas_silver(entidade: Entidade) -> list[tuple[str, str]]:
    """Colunas do modelo Silver: (expressão, alias)."""
    colunas: list[tuple[str, str]] = []
    vistos: set[str] = set()
    for mapeamento in entidade.chaves:
        colunas.append(
            (
                f'{{{{ {PACOTE_BASE}.chave_conformada("{mapeamento.chave}", "{mapeamento.coluna}") }}}}',
                mapeamento.chave,
            )
        )
        vistos.add(mapeamento.chave)
    for coluna in (
        list(entidade.chave_primaria)
        + [m.coluna for m in entidade.chaves]
        + [COLUNA_INGESTAO]
    ):
        if coluna not in vistos:
            colunas.append((coluna, coluna))
            vistos.add(coluna)
    return colunas


def _sql_silver(sistema: Sistema, entidade: Entidade) -> str:
    colunas = _colunas_silver(entidade)
    projecao = ",\n".join(
        f"        {expr} as {alias}" if expr != alias else f"        {alias}"
        for expr, alias in colunas
    )
    saida = ",\n".join(f"    {alias}" for _, alias in colunas)

    chave = list(entidade.chave_primaria) or [a for _, a in colunas[:1]]
    particao = ", ".join(chave)

    cabecalho = [
        f"-- Silver de {sistema.id}.{entidade.id} — verdade única do dado (ADR-0006).",
        "--",
        f"-- Granularidade: {entidade.granularidade}.",
        "--",
        "-- Gerado por `make modelo` a partir do catálogo. As chaves conformadas e a",
        "-- deduplicação já vêm prontas; acrescente à mão as colunas de negócio que",
        "-- este modelo precisa expor.",
    ]
    for mapeamento in entidade.chaves:
        if not mapeamento.verificado:
            cabecalho.append(
                f"-- ATENÇÃO: o mapeamento {mapeamento.chave} → {mapeamento.coluna} ainda"
            )
            cabecalho.append("--          não foi verificado contra o dado ingerido.")

    nome_source = (
        f"{sistema.id}_bronze"
        if entidade.schema_origem == sistema.schema_bronze
        else sistema.id
    )
    fonte = f'{{{{ source("{nome_source}", "{entidade.id}") }}}}'
    linhas = [
        *cabecalho,
        "",
        "with",
        f"bronze as (select * from {fonte}),",
        "",
        "conformado as (",
        "    select",
        projecao,
        "    from bronze",
        "),",
        "",
        "versionado as (",
        "    select",
        "        conformado.*,",
        "        row_number() over (",
        f"            partition by {particao} order by {COLUNA_INGESTAO} desc",
        "        ) as nu_versao",
        "    from conformado",
        ")",
        "",
        "select",
        saida,
        "from versionado",
        "where nu_versao = 1",
        "",
    ]
    return "\n".join(linhas)


def _yaml_modelo(
    catalogo: Catalogo,
    nome: str,
    descricao: str,
    granularidade: str,
    owner: str,
    sistema_origem: str,
    camada: str,
    colunas: list[Coluna],
) -> str:
    """Entrada de schema.yml para um modelo, com os metadados do ADR-0013."""
    classificacao = classificacao_mais_restritiva([c.classificacao for c in colunas])
    linhas = [
        f"  - name: {nome}",
        "    description: >",
        _quebrar(f"{descricao} Granularidade: {granularidade}.", recuo="      "),
        "    meta:",
        f'      owner: "{owner}"',
        f"      sistema_origem: {sistema_origem}",
        f"      camada: {camada}",
        f"      classificacao: {classificacao}",
        "    columns:",
    ]
    for coluna in colunas:
        linhas += [
            f"      - name: {coluna.nome}",
            "        description: >",
            _quebrar(coluna.descricao, recuo="            "),
            "        meta:",
            f"          classificacao: {coluna.classificacao}",
        ]
        if coluna.testar:
            linhas += ["        data_tests:", "          - not_null"]
        elif coluna.motivo_sem_teste and coluna.nome in catalogo.chaves:
            linhas.append(f"        # sem teste not_null: {coluna.motivo_sem_teste}")
            linhas.append("        # (ver catalogo/sistemas/).")
    linhas.append("")
    return "\n".join(linhas)


def _descricao_coluna(
    catalogo: Catalogo, nome: str, entidade: Entidade
) -> tuple[str, str]:
    """Descrição e classificação de uma coluna, resolvidas pelo catálogo."""
    chave: Chave | None = catalogo.chaves.get(nome)
    if chave is not None:
        origem = next((m for m in entidade.chaves if m.chave == nome), None)
        sufixo = (
            f" Normalizada a partir da coluna de origem '{origem.coluna}'."
            if origem
            else ""
        )
        ressalva = f" {origem.observacao}" if origem and origem.observacao else ""
        return f"{chave.nome}. {chave.descricao}{sufixo}{ressalva}", chave.classificacao
    if nome == COLUNA_INGESTAO:
        return (
            "Data e hora em que o registro foi gravado na Bronze pela DAG de ingestão.",
            "publico",
        )
    return (
        f"PREENCHER: o que a coluna '{nome}' significa, na origem "
        f"{entidade.origem_atual or entidade.id}.",
        entidade.classificacao,
    )


def _acrescentar_ao_schema(
    caminho: Path, nome_modelo: str, bloco: str, forcar: bool
) -> Escrita:
    """Insere (ou substitui) a entrada de um modelo no schema.yml da pasta."""
    if not caminho.exists():
        cabecalho = "version: 2\n\nmodels:\n"
        caminho.parent.mkdir(parents=True, exist_ok=True)
        caminho.write_text(cabecalho + bloco, encoding="utf-8")
        return Escrita(caminho, "criado")

    atual = caminho.read_text(encoding="utf-8")
    padrao = re.compile(
        rf"^  - name: {re.escape(nome_modelo)}$.*?(?=^  - name: |\Z)",
        re.MULTILINE | re.DOTALL,
    )
    if padrao.search(atual):
        if not forcar:
            return Escrita(caminho, "preservado")
        caminho.write_text(padrao.sub(bloco, atual, count=1), encoding="utf-8")
        return Escrita(caminho, "atualizado")

    separador = "" if atual.endswith("\n") else "\n"
    caminho.write_text(atual + separador + bloco, encoding="utf-8")
    return Escrita(caminho, "atualizado")


def gerar_silver(
    catalogo: Catalogo, id_sistema: str, id_entidade: str, forcar: bool = False
) -> list[Escrita]:
    """Gera o modelo Silver de uma entidade e sua documentação."""
    sistema = catalogo.sistema(id_sistema)
    entidade = catalogo.entidade(id_sistema, id_entidade)
    if not entidade.dominio:
        raise ErroCatalogo(
            f"Entidade '{id_sistema}.{id_entidade}' não declara 'dominio' — "
            "sem ele não há pasta nem schema Silver (ADR-0009, ADR-0010)."
        )

    base = caminho_pacote(sistema.id) / "models" / "silver" / entidade.dominio
    escritas = [
        _escrever(base / f"{entidade.id}.sql", _sql_silver(sistema, entidade), forcar)
    ]

    por_chave = _chaves_de(entidade)
    colunas = [
        Coluna(
            alias,
            *_descricao_coluna(catalogo, alias, entidade),
            *_decidir_teste(por_chave.get(alias)),
        )
        for _, alias in _colunas_silver(entidade)
    ]
    bloco = _yaml_modelo(
        catalogo,
        nome=entidade.id,
        descricao=entidade.descricao,
        granularidade=entidade.granularidade,
        owner=sistema.owner,
        sistema_origem=sistema.id,
        camada="silver",
        colunas=colunas,
    )
    escritas.append(
        _acrescentar_ao_schema(base / "schema.yml", entidade.id, bloco, forcar)
    )
    return escritas


# --------------------------------------------------------------------------
# Gold — o cruzamento
# --------------------------------------------------------------------------


def _chaves_de(entidade: Entidade) -> dict[str, Mapeamento]:
    return {m.chave: m for m in entidade.chaves}


def planejar_cruzamento(catalogo: Catalogo, referencias: list[str]) -> list[Passo]:
    """Resolve como as entidades informadas se ligam, via chaves conformadas.

    A primeira referência é a base; cada seguinte entra por uma chave que já
    exista no conjunto acumulado — diretamente, ou por uma ponte declarada em
    ``chaves.yml``. Referência que não se liga a nada interrompe o plano com
    erro, em vez de gerar um produto cartesiano silencioso.
    """
    if len(referencias) < 2:
        raise ErroCatalogo(
            "Um modelo Gold de cruzamento precisa de ao menos duas entidades "
            "(--cruzar sistema.entidade, repetido)."
        )

    resolvidas: list[tuple[Sistema, Entidade]] = []
    for referencia in referencias:
        if "." not in referencia:
            raise ErroCatalogo(
                f"Referência '{referencia}' deve estar no formato 'sistema.entidade'."
            )
        id_sistema, id_entidade = referencia.split(".", 1)
        resolvidas.append(
            (catalogo.sistema(id_sistema), catalogo.entidade(id_sistema, id_entidade))
        )

    multiplos_sistemas = len({s.id for s, _ in resolvidas}) > 1

    def alias_de(sistema: Sistema, entidade: Entidade) -> str:
        return f"{sistema.id}_{entidade.id}" if multiplos_sistemas else entidade.id

    sistema_base, entidade_base = resolvidas[0]
    disponiveis: dict[str, str] = {
        chave: alias_de(sistema_base, entidade_base)
        for chave in _chaves_de(entidade_base)
    }
    if not disponiveis:
        raise ErroCatalogo(
            f"A entidade base '{sistema_base.id}.{entidade_base.id}' não expõe "
            "nenhuma chave conformada — não há por onde cruzar."
        )

    passos: list[Passo] = []
    for sistema, entidade in resolvidas[1:]:
        chaves = _chaves_de(entidade)
        alias = alias_de(sistema, entidade)

        direta = next((c for c in chaves if c in disponiveis), None)
        if direta is not None:
            passos.append(
                Passo(
                    sistema=sistema,
                    entidade=entidade,
                    alias=alias,
                    esquerda_alias=disponiveis[direta],
                    chave_esquerda=direta,
                    chave_direita=direta,
                )
            )
        else:
            via = next(
                (
                    (p, p.de)
                    for p in catalogo.pontes
                    if p.de in disponiveis and p.para in chaves
                ),
                None,
            )
            if via is None:
                disponivel = ", ".join(sorted(disponiveis)) or "nenhuma"
                oferecidas = ", ".join(sorted(chaves)) or "nenhuma"
                raise ErroCatalogo(
                    f"'{sistema.id}.{entidade.id}' não se liga às entidades anteriores: "
                    f"chaves disponíveis são [{disponivel}] e ela expõe [{oferecidas}]. "
                    "Declare uma ponte em catalogo/chaves.yml ou inclua uma entidade "
                    "intermediária."
                )
            ponte, chave_esquerda = via
            passos.append(
                Passo(
                    sistema=sistema,
                    entidade=entidade,
                    alias=alias,
                    esquerda_alias=disponiveis[chave_esquerda],
                    chave_esquerda=chave_esquerda,
                    chave_direita=ponte.para,
                    ponte=ponte,
                )
            )

        for chave in chaves:
            disponiveis.setdefault(chave, alias)

    return passos


def _sql_gold(
    catalogo: Catalogo, nome: str, referencias: list[str], passos: list[Passo]
) -> str:
    id_sistema_base, id_entidade_base = referencias[0].split(".", 1)
    sistema_base = catalogo.sistema(id_sistema_base)
    entidade_base = catalogo.entidade(id_sistema_base, id_entidade_base)
    alias_base = passos[0].esquerda_alias if passos else entidade_base.id

    ctes = [
        f'{alias_base} as (select * from {{{{ ref("{sistema_base.id}", '
        f'"{entidade_base.id}") }}}}),'
    ]
    for passo in passos:
        ctes.append(
            f'{passo.alias} as (select * from {{{{ ref("{passo.sistema.id}", '
            f'"{passo.entidade.id}") }}}}),'
        )

    joins = []
    extras = []
    for passo in passos:
        if passo.ponte is not None:
            joins.append(
                f"        -- ponte {passo.ponte.de} → {passo.ponte.para} "
                f"(confiabilidade: {passo.ponte.confiabilidade})"
            )
            if passo.ponte.condicao:
                # Sem a condição à vista, o join parece cobrir toda a base: uma
                # derivação condicional só vale para o subconjunto que a satisfaz,
                # e as demais linhas somem sem aviso no left join.
                joins.append(
                    f"        -- a derivação só vale onde {passo.ponte.condicao}; "
                    "as demais linhas ficam sem par."
                )
            if passo.ponte.confiabilidade != "total":
                joins.append(
                    "        -- a cobertura deste join não é total: meça a taxa de "
                    "não-correspondência."
                )
        joins.append(
            f"        left join {passo.alias} "
            f"on {passo.esquerda_alias}.{passo.chave_esquerda} "
            f"= {passo.alias}.{passo.chave_direita}"
        )
        for chave in _chaves_de(passo.entidade):
            if chave != passo.chave_direita:
                extras.append(f"            {passo.alias}.{chave}")

    projecao = [f"            {alias_base}.*"] + extras
    avisos = [
        f"-- ATENÇÃO: o cruzamento com {passo.sistema.id}.{passo.entidade.id} usa a ponte"
        f" {passo.ponte.de} → {passo.ponte.para}, de confiabilidade"
        f" {passo.ponte.confiabilidade}."
        for passo in passos
        if passo.ponte is not None
    ]

    cabecalho = [
        f"-- Gold: {nome} — produto de dados (ADR-0006).",
        "--",
        "-- Cruzamento gerado por `make modelo` a partir das chaves conformadas do",
        f"-- catálogo: {' ⨝ '.join(referencias)}.",
        "--",
        "-- PREENCHER: a pergunta de negócio que esta tabela responde, e a regra de",
        "-- cálculo de cada coluna derivada (ADR-0013).",
        "--",
        "-- O cruzamento traz apenas as chaves; as colunas de negócio de cada entidade",
        "-- ficam disponíveis para projetar em: "
        + ", ".join(f"{p.alias}.*" for p in passos)
        + ".",
        *avisos,
    ]

    linhas = [
        *cabecalho,
        "",
        "with",
        *ctes,
        "",
        "cruzado as (",
        "        select",
        ",\n".join(projecao),
        f"        from {alias_base}",
        *joins,
        ")",
        "",
        "select *",
        "from cruzado",
        "",
    ]
    return "\n".join(linhas)


def gerar_gold(
    catalogo: Catalogo,
    orgao: str,
    produto: str,
    nome: str,
    referencias: list[str],
    forcar: bool = False,
) -> list[Escrita]:
    """Gera um modelo Gold cruzando as entidades informadas."""
    passos = planejar_cruzamento(catalogo, referencias)
    base = caminho_projeto(orgao) / "models" / "gold" / produto

    escritas = [
        _escrever(
            base / f"{nome}.sql", _sql_gold(catalogo, nome, referencias, passos), forcar
        )
    ]

    id_sistema_base, id_entidade_base = referencias[0].split(".", 1)
    sistema_base = catalogo.sistema(id_sistema_base)
    entidade_base = catalogo.entidade(id_sistema_base, id_entidade_base)

    colunas: list[Coluna] = []

    def acrescentar(entidade: Entidade) -> None:
        for chave, mapeamento in _chaves_de(entidade).items():
            if chave in {c.nome for c in colunas}:
                continue
            colunas.append(
                Coluna(
                    chave,
                    *_descricao_coluna(catalogo, chave, entidade),
                    *_decidir_teste(mapeamento),
                )
            )

    acrescentar(entidade_base)
    for passo in passos:
        acrescentar(passo.entidade)

    sistemas = sorted({sistema_base.id} | {p.sistema.id for p in passos})
    bloco = _yaml_modelo(
        catalogo,
        nome=nome,
        descricao=(
            f"PREENCHER: a pergunta de negócio respondida por este produto de dados. "
            f"Construído cruzando {', '.join(referencias)}."
        ),
        granularidade=f"herdada de {referencias[0]} — {entidade_base.granularidade}",
        owner=sistema_base.owner,
        sistema_origem=", ".join(sistemas),
        camada="gold",
        colunas=colunas,
    )
    escritas.append(_acrescentar_ao_schema(base / "schema.yml", nome, bloco, forcar))
    return escritas
