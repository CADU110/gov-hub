{#
    Unicidade de uma combinação de colunas — a chave primária composta que a
    maioria das entidades dos sistemas estruturantes tem. Evita depender de um
    pacote externo para o teste mais comum do framework.

        data_tests:
          - gov_bricks.unicidade_composta:
              colunas: [co_uasg, numerocontrato, nu_ni]
#}
{% test unicidade_composta(model, colunas) %}
    select {{ colunas | join(", ") }}, count(*) as qt_registros
    from {{ model }}
    group by {{ colunas | join(", ") }}
    having count(*) > 1
{% endtest %}

{#
    Mesmo número de linhas que outro modelo. Serve à Silver inicial de um
    domínio em Parquet, que ainda não filtra nem deduplica a Bronze.

        data_tests:
          - gov_bricks.mesma_contagem:
              comparar_com: ref("arp")
#}
{% test mesma_contagem(model, comparar_com) %}
    select contagem_modelo, contagem_comparada
    from (select count(*) as contagem_modelo from {{ model }}) as modelo
    cross join (select count(*) as contagem_comparada from {{ comparar_com }}) as comparado
    where contagem_modelo != contagem_comparada
{% endtest %}

{#
    Mesmo conteúdo que outro modelo, só com os nomes de coluna em snake_case:
    é o contrato da Silver inicial de um domínio em Parquet. Compara linha a
    linha nos dois sentidos com EXCEPT ALL, o que pega valor alterado, nulo
    novo e duplicata perdida. As colunas vêm do modelo comparado, e não de uma
    lista no teste, para que o mesmo teste sirva a qualquer domínio. Quando a
    Silver passar a tratar o dado, este teste deixa de valer e deve sair.

        data_tests:
          - gov_bricks.mesmo_conteudo_em_snake_case:
              comparar_com: ref("arp")
#}
{% test mesmo_conteudo_em_snake_case(model, comparar_com) %}
    {%- set colunas = adapter.get_columns_in_relation(comparar_com) if execute else [] -%}
    {%- if execute and not colunas -%}
        {{ exceptions.raise_compiler_error("Sem colunas para comparar em " ~ comparar_com) }}
    {%- endif -%}
    with
        comparado as (
            select
                {%- for coluna in colunas %}
                    {{ adapter.quote(coluna.name) }} as {{ gov_bricks.snake_case(coluna.name) }}
                    {{- "," if not loop.last }}
                {%- endfor %}
            from {{ comparar_com }}
        ),
        modelo as (
            select
                {%- for coluna in colunas %}
                    {{ gov_bricks.snake_case(coluna.name) }}{{ "," if not loop.last }}
                {%- endfor %}
            from {{ model }}
        ),
        so_no_comparado as (
            select *
            from comparado
            except all
            select *
            from modelo
        ),
        so_no_modelo as (
            select *
            from modelo
            except all
            select *
            from comparado
        )
    select *
    from so_no_comparado
    union all
    select *
    from so_no_modelo
{% endtest %}

{#
    camelCase para snake_case, tratando siglas como uma palavra:
    numeroAtaRegistroPreco → numero_ata_registro_preco, linkAtaPNCP → link_ata_pncp.
#}
{% macro snake_case(nome) %}
    {%- set nome = modules.re.sub("([A-Z]+)([A-Z][a-z])", "\\1_\\2", nome) -%}
    {%- set nome = modules.re.sub("([a-z0-9])([A-Z])", "\\1_\\2", nome) -%}
    {{ return(nome | lower) }}
{% endmacro %}
