{#
    Bronze de uma entidade da zona raw em Parquet, lida pelo DuckDB (target
    prod_duckdb) e gravada no PostgreSQL anexado (ADR-0021).

    O caminho segue uma convenção, não argumentos: o pacote do modelo é o
    sistema, e o nome do modelo é a entidade. É o mesmo par que a ingestão usa
    em `write_raw(sistema, entidade)` e em `landing_zone.asset_raw(...)`. A
    DAG `duckdb_<orgao>_transform_dag` deriva da mesma convenção os Assets que
    a agendam. Por isso uma Bronze nova é só o arquivo
    `models/bronze/<entidade>.sql` contendo `{{ gov_bricks.bronze_parquet() }}`.

    `union_by_name`: Parquets de execuções diferentes podem trazer colunas
    diferentes, e a leitura posicional as embaralharia.
#}
{% macro bronze_parquet() %}
    {%- set bucket = env_var("MINIO_BUCKET", "data-lake") -%}
    {%- set caminho = ("s3://" ~ bucket ~ "/" ~ model.package_name ~ "/" ~ model.name ~ "/**/*.parquet") | replace(
        "'", "''"
    ) -%}
    select *
    from read_parquet('{{ caminho }}', union_by_name = true)
{% endmacro %}
