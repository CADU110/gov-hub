# gov-bricks

**gov-bricks** é o `data-framework` do [Gov Hub BR](https://gov-hub.io/govhub/):
uma base compartilhada e opinativa para projetos de dados públicos brasileiros,
cobrindo ingestão, transformação, qualidade e publicação de dados.

Em vez de cada projeto reimplementar sua própria stack, os projetos consumidores
herdam deste repositório a estrutura, as convenções e os utilitários comuns —
ver [ADR-0003](docs/adr/0003-govhub-como-framework-compartilhado-de-dados.md).
O repositório é um **monorepo**: framework e código dos projetos consumidores
convivem aqui, organizados por sistema e órgão
([ADR-0004](docs/adr/0004-monorepo-como-estrategia-de-organizacao-de-codigo.md)).

## Stack

| Camada | Ferramenta |
|---|---|
| Orquestração | Apache Airflow 2.8 ([ADR-0001](docs/adr/0001-airflow-como-orquestrador-de-fluxos-de-dados.md)) |
| Transformação | dbt + Astronomer Cosmos ([ADR-0002](docs/adr/0002-dbt-como-ferramenta-de-transformacao-de-dados.md)) |
| Landing zone | Object storage — MinIO on-prem, S3/ADLS na nuvem ([ADR-0012](docs/adr/0012-ingestao-object-storage-vs-database.md)) |
| Modelagem | Arquitetura medallion: bronze → silver → gold ([ADR-0006](docs/adr/0006-arquitetura-medallion.md)) |
| Cruzamento | Chaves conformadas declaradas em [`catalogo/`](catalogo/README.md) ([ADR-0017](docs/adr/0017-chaves-conformadas-cruzamento-sistemas-estruturantes.md)) |
| Consumo | Apache Superset, com dashboards versionadas e acesso derivado do catálogo ([ADR-0019](docs/adr/0019-publicacao-dashboards-relatorios.md), [ADR-0020](docs/adr/0020-niveis-acesso-consumo-dados.md)) |
| Dependências | `uv` + `pyproject.toml` |
| Qualidade | `black`, `ruff`, `ty`, `sqlfmt`, `pytest` |

## Estrutura

```
airflow/
  dags/
    data_ingest/<sistema>/[<orgao>/]   # DAGs de ingestão
    data_ingest/<orgao>/               # sistema interno de um único órgão
    data_transform/<orgao>/            # DAGs de transformação (dbt via Cosmos)
    data_publish/<orgao>/              # DAGs de publicação (dashboards e acesso)
    data_report/<orgao>/               # DAGs de relatório periódico
    dbt/gov_bricks/                    # pacote base: macros compartilhados
    dbt/<sistema>/                     # pacote dbt compartilhado
    dbt/<orgao>/                       # projeto dbt do órgão
    superset/<orgao>/                  # bundles de dashboard + plano de acesso gerado
    homologation/                      # DAGs de qualidade/homologação
  helpers/                             # utilitários Python compartilhados
  plugins/                             # clientes de fonte e integrações
catalogo/                              # sistemas estruturantes e chaves de cruzamento
catalogo/publicacao/                   # o que cada órgão publica e quem consome
scripts/modelagem/                     # gerador de modelos e mapa de cruzamento
scripts/publicacao/                    # validação de publicação e plano de acesso
docker/                                # Dockerfile, docker-compose.yml, init do Postgres
docs/adr/                              # decisões de arquitetura
tests/                                 # unit (CI) e integration (docker compose)
```

As convenções de nomenclatura de pastas e arquivos estão nos ADRs
[0007](docs/adr/0007-nomenclatura-pastas-dags-ingestao.md) (DAGs de ingestão),
[0009](docs/adr/0009-nomenclatura-pastas-arquivos-dbt.md) (dbt) e
[0010](docs/adr/0010-nomenclatura-schemas-tabelas-bronze-silver-gold.md)
(schemas e tabelas),
[0018](docs/adr/0018-nomenclatura-execucao-dags-transformacao.md) (DAGs de
transformação) e
[0019](docs/adr/0019-publicacao-dashboards-relatorios.md) (DAGs de publicação e
de relatório).

## Utilitários compartilhados

### `airflow/plugins/`

| Módulo | O que faz |
|---|---|
| `cliente_base.py` | Cliente HTTP base (`httpx`) com retry e backoff exponencial |
| `cliente_contratos_gov.py` | API aberta do Contratos.gov.br: achata o cabeçalho do contrato e injeta `contrato_id` nos sub-recursos |
| `cliente_postgres.py` | Cliente PostgreSQL: criação de tabela por inferência de tipos, upsert, deduplicação |
| `cliente_sqlserver.py` | Leitura de tabelas SQL Server via `MsSqlHook` |
| `cliente_storage.py` | Abstração `fsspec` da landing zone (`STORAGE_BACKEND`: `minio`/`s3`/`adls`) |
| `cliente_email.py` | Extração de anexos CSV/ZIP de caixas IMAP e envio de relatório por SMTP |
| `cliente_superset.py` | API do Superset: import de bundle, papéis, permissões e recorte de linhas |
| `email_ingest_dag_factory.py` | Factory de DAG para ingestão de relatórios recebidos por email |
| `relatorio_dag_factory.py` | Factory da DAG base de relatório: uma entrega por órgão consumidor |
| `schedule_loader.py` | Schedules dinâmicos via Airflow Variable `dynamic_schedules` |

### `airflow/helpers/`

| Módulo | O que faz |
|---|---|
| `landing_zone.py` | `build_landing_path`, `write_parquet`, `read_parquet`, `list_files` |
| `homologation_helpers.py` | Checks de qualidade em Polars (`not_null`, `schema`, `row_count`, `no_duplicates`, `null_rate`) |
| `homologation_flow.py` | Fluxo landing zone → validação → Postgres |
| `postgres_helpers.py` | Resolução de connection string a partir de uma connection do Airflow |
| `retry_helpers.py` | Decorator `retry_on_exception` com backoff |
| `safe_request.py` | Variante de request tolerante a 204, corpo vazio e JSON inválido |
| `superset_helpers.py` | Cliente do Superset a partir da connection `superset_default` |

Convenção da landing zone:
`{bucket}/{source}/{entity}/{ano}/{mes}/{dia}/{run_id}.parquet`

## Sistemas estruturantes

Cada sistema é catalogado em [`catalogo/sistemas/`](catalogo/README.md), que é a
fonte de verdade sobre suas entidades e sobre por quais chaves elas se cruzam.
`ingerido` significa que há DAG de ingestão neste repositório; `planejado`, que o
sistema está catalogado para o roadmap.

| Sistema | Órgão gestor | Entidades | Status |
|---|---|---|---|
| [`compras_gov`](catalogo/sistemas/compras_gov.yml) — Compras.gov.br (SIASG) | MGI | 8 | ingerido |
| [`contratos_gov`](catalogo/sistemas/contratos_gov.yml) — Contratos.gov.br | MGI | 18 | planejado |

Decisões operacionais da ingestão do `contratos_gov` — concorrência medida,
horários e escopo de detalhamento — em
[`docs/notas/contratos-gov-ingestao.md`](docs/notas/contratos-gov-ingestao.md).

## Modelagem e cruzamento entre sistemas

O valor da plataforma está em cruzar sistemas estruturantes, e esse cruzamento é
**declarado**, não improvisado a cada modelo. O catálogo em [`catalogo/`](catalogo/README.md)
registra quais entidades cada sistema expõe e por quais **chaves conformadas**
elas se ligam — `co_orgao`, `co_uasg`, `nu_cnpj`, `nu_matricula_siape` — com a
normalização de cada uma e a confiabilidade de cada equivalência conhecida
([ADR-0017](docs/adr/0017-chaves-conformadas-cruzamento-sistemas-estruturantes.md)).

```bash
make mapa
```

> O que se liga a quê, por qual chave, e o que ainda é hipótese não verificada.

```bash
make modelo ARGS="--camada silver --sistema compras_gov --entidade contratos"
```

> Gera o modelo dbt na pasta do [ADR-0009](docs/adr/0009-nomenclatura-pastas-arquivos-dbt.md),
> com as chaves já normalizadas e o `schema.yml` com os metadados obrigatórios do
> [ADR-0013](docs/adr/0013-padrao-documentacao-metadados-tabelas.md).

```bash
make modelo ARGS="--camada gold --orgao mgi --produto contratacoes \
                  --entidade contratos_por_orgao \
                  --cruzar compras_gov.contratos --cruzar compras_gov.uasg"
```

> Resolve o plano de join pelo catálogo — inclusive por caminho transitivo ou por
> ponte entre chaves distintas. Entidade que não se liga a nenhuma outra falha na
> geração, em vez de virar um produto cartesiano silencioso.

O schema de destino é derivado da pasta do modelo pelo macro
`generate_schema_name`, no padrão do
[ADR-0010](docs/adr/0010-nomenclatura-schemas-tabelas-bronze-silver-gold.md):
`silver/contratacoes/contratos.sql` → `002_slv_contratacoes.contratos`.

A Bronze não é modelada em dbt: ela é materializada pelas DAGs de ingestão e
declarada como `source` gerado a partir do catálogo — ver ADR-0017.

## Execução da transformação

Quem roda os modelos é uma DAG de transformação por órgão, em
`data_transform/<orgao>/{orgao}_transform_dag.py`
([ADR-0018](docs/adr/0018-nomenclatura-execucao-dags-transformacao.md)). Ela usa
[Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/) para
converter o grafo do projeto dbt em tasks do Airflow — **uma task por modelo e
por teste**, com as dependências herdadas dos próprios `ref()`.

Na prática isso significa que um modelo Gold que falha não obriga a reprocessar
a Silver inteira no retry, e que acrescentar um modelo ao projeto dbt não exige
mudança nenhuma no arquivo da DAG.

> Os pacotes locais em `packages.yml` são referenciados via
> `{{ env_var('GOV_BRICKS_DBT_DIR', '..') }}` porque o Cosmos renderiza o projeto
> em um diretório temporário para montar o grafo — de lá, um caminho relativo
> apontaria para fora do monorepo.

## Publicação: dashboards, relatórios e acesso

Publicar é uma etapa versionada do pipeline, não um trabalho manual na
ferramenta de BI
([ADR-0019](docs/adr/0019-publicacao-dashboards-relatorios.md)). O catálogo em
`catalogo/publicacao/<orgao>.yml` declara o que o órgão publica — datasets,
dashboards, relatórios — e **quem consome cada coisa**; os bundles em
`airflow/dags/superset/<orgao>/` são o que efetivamente vai ao ar.

```bash
make acesso                # quem enxerga o quê, e com que recorte de linhas
make publicacao-sync       # regera os planos de acesso lidos pelas DAGs
make publicacao-validar    # roda dentro de make lint e no CI
make superset              # sobe o Superset local (perfil `bi` do compose)
```

**Dashboards.** São construídas na interface do Superset, exportadas como
bundle de YAML e versionadas. A DAG `data_publish/<orgao>/{orgao}_publish_dag.py`
importa o bundle pela API, de forma idempotente. A conexão do warehouse no
bundle é um placeholder: a DAG a substitui pela URI do ambiente
(`SUPERSET_DW_URI`) e envia a senha por fora — nenhuma credencial é versionada.

**Relatórios.** A DAG `data_report/<orgao>/{relatorio}_{orgao}_report_dag.py`
usa a factory `relatorio_dag_factory`: a consulta é escrita uma vez, com a marca
`{recorte}`, e executada uma vez por órgão consumidor — cada um recebendo apenas
as suas linhas, na landing zone
(`{bucket}/relatorios/{orgao}/{relatorio}/{ano}/{mes}/{dia}/`), por e-mail, ou
ambos. A lista de destinatários fica em uma Variable do Airflow, nunca no
repositório: endereço de pessoa é dado pessoal.

O ambiente local precisa, uma vez: as connections `superset_default` e
`postgres_dw` (ambas criadas por `make dev`) e, para relatório com destino
`email`, as Variables `smtp_credentials` e a de destinatários declarada no
catálogo.

**Níveis de acesso.** Cada consumidor declarado vira um papel
`gh_{orgao}_{nivel}` no Superset, e quem tem `abrangencia: proprio` ganha um
filtro de linhas por `co_orgao`
([ADR-0020](docs/adr/0020-niveis-acesso-consumo-dados.md)). O nível diz *quais
classificações* a pessoa vê, no vocabulário do
[ADR-0013](docs/adr/0013-padrao-documentacao-metadados-tabelas.md) — e o CI
reprova o build quando um dataset publicado expõe coluna classificada acima do
seu nível, ou coluna que ninguém documentou.

## Começando

Pré-requisitos: Python 3.11, [uv](https://docs.astral.sh/uv/getting-started/installation/),
Docker, Docker Compose e Make.

```bash
make setup      # instala deps, gera requirements.txt, cria .env e instala git hooks
make compose    # sobe Airflow + Postgres + MinIO e configura variables/connections
```

Airflow em http://localhost:8080 (`airflow`/`airflow`), console do MinIO em
http://localhost:9001 (`minioadmin`/`minioadmin`).

O Superset fica em um perfil separado do compose, porque é o serviço mais
pesado e a maior parte do trabalho no framework não precisa dele:

```bash
make superset   # http://localhost:8088 (admin/admin) + connection no Airflow
```

## Comandos

| Comando | Descrição |
|---|---|
| `make install` | Instala as dependências com `uv sync` |
| `make requirements` | Regenera o `requirements.txt` (runtime) usado pela imagem Docker |
| `make format` | Aplica `black`, `ruff --fix` e `sqlfmt` |
| `make lint` | Verifica `black`, `ruff`, `ty`, `sqlfmt` e os catálogos de modelagem e publicação |
| `make test` | Testes unitários com cobertura (o que roda no CI) |
| `make test-integration` | Testes de integração (sobe MinIO e Postgres) |
| `make dev` / `make dev-check` | Configura e valida variables/connections do Airflow local |
| `make mapa` | Mapa de cruzamento entre sistemas estruturantes (`ARGS="--mermaid"` para grafo) |
| `make modelo` | Gera modelos dbt a partir do catálogo (ver `ARGS` acima) |
| `make catalogo-validar` | Valida o catálogo — roda dentro de `make lint` e no CI |
| `make catalogo-sync` | Regera os macros dbt derivados de `catalogo/chaves.yml` |
| `make acesso` | Matriz de acesso: quem enxerga qual dashboard, com que recorte |
| `make publicacao-validar` | Valida o catálogo de publicação — roda dentro de `make lint` e no CI |
| `make publicacao-sync` | Regera os planos de acesso lidos pelas DAGs de publicação |
| `make superset` | Sobe o Superset local (perfil `bi` do compose) |

> `requirements.txt` é um artefato gerado por `make requirements` — não edite à
> mão. Dependências entram no `pyproject.toml`.

## Contribuindo

Leia o [Guia de Contribuição](.github/CONTRIBUTING.md) e o
[Protocolo de Aprovação de Pull Requests](.github/MERGE_REQUEST_PROTOCOL.md).
Commits e títulos de PR seguem [Conventional Commits](.github/TEMPLATES/COMMIT_TEMPLATE.md).
A revisão obrigatória por pasta é definida em [`.github/CODEOWNERS`](.github/CODEOWNERS).

## Licença

[MIT](LICENSE).
