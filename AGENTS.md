# AGENTS.md — guia para agentes de código no gov-bricks

Este arquivo é o ponto de entrada para um agente (Claude Code, Codex, Cursor,
Copilot etc.) que vai desenvolver neste repositório. Ele condensa o que um
contribuidor experiente já sabe e que não está óbvio no código: como o
repositório se organiza, quais regras o CI aplica, quais decisões já foram
tomadas e quais armadilhas já custaram horas de depuração.

Leia junto, quando o assunto pedir:

- [README.md](README.md) — visão geral, stack, comandos.
- [docs/adr/](docs/adr/README.md) — as decisões de arquitetura. **Quando uma
  regra deste arquivo cita um ADR, o ADR é a fonte de verdade.**
- [.github/CONTRIBUTING.md](.github/CONTRIBUTING.md) e
  [.github/MERGE_REQUEST_PROTOCOL.md](.github/MERGE_REQUEST_PROTOCOL.md) —
  fluxo de PR, revisão por domínio e commits.
- [catalogo/README.md](catalogo/README.md) — catálogo de chaves conformadas
  e de publicação.

---

## 1. O que é este repositório

`gov-bricks` é o **data-framework do Gov Hub BR**: uma base compartilhada
para pipelines de dados públicos brasileiros, em **monorepo** (ADR-0003,
ADR-0004). Código de framework (plugins, helpers, macros dbt, catálogo) e
código dos órgãos consumidores (DAGs e projetos dbt por órgão) convivem aqui,
separados por pasta.

Stack: **Apache Airflow 3.2** (TaskFlow via `airflow.sdk`), **dbt 1.12 +
Astronomer Cosmos**, **Postgres** como warehouse local, **MinIO** como object
storage local, **Superset** para consumo. Python 3.11 fixo, dependências via
`uv`.

A cadeia completa que existe hoje, ponta a ponta, é a do sistema
**Compras.gov.br** para o órgão **MGI**:

```
23 DAGs data_ingest/compras_gov/*  ──write_raw()──▶  zona raw (RAW_BACKEND)
        01:00–05:00 (ARP às 06:00/07:00)              │
                                                      ▼ source dbt (Bronze)
data_transform/mgi/mgi_transform_dag  ──Cosmos──▶  Silver (compras_gov) + Gold (mgi)
        06:00                                         │
                              ┌───────────────────────┴───────────────────────┐
                              ▼                                               ▼
data_publish/mgi/mgi_publish_dag (08:00)              data_report/mgi/contratacoes_mgi_report_dag
importa bundles no Superset + papéis + recorte         (dia 1º, 07:00) 1 entrega por órgão consumidor
```

---

## 2. Comandos que importam

```bash
make setup             # uv sync + requirements.txt + .env + git hooks (uma vez)
make format            # black, ruff --fix, sqlfmt
make lint              # black --check, ruff, ty, sqlfmt --check, catalogo-validar, publicacao-validar
make test              # pytest tests/unit com cobertura — é o que roda no CI
make test-integration  # sobe MinIO + Postgres e roda tests/integration
make compose           # sobe Airflow + Postgres + MinIO e cria variables/connections
make superset          # sobe o Superset (perfil bi do compose) — pesado, só quando precisar
```

Ferramentas de catálogo (ver seções 5 e 6):

```bash
make catalogo-validar / catalogo-sync / mapa / modelo ARGS="..."
make publicacao-validar / publicacao-sync / acesso
```

**Antes de considerar uma tarefa pronta: `make lint && make test` verdes.**
O CI (`.github/workflows/main.yml`) roda exatamente esses dois alvos e depois
faz o build da imagem Docker. O hook de pre-push instalado por `make setup`
roda os mesmos dois. Estado em 2026-09-03: ambos verdes, 281 testes.

Rodar um único teste ou uma DAG:

```bash
uv run pytest tests/unit/test_batching.py -q
docker compose -f docker/docker-compose.yml exec airflow airflow dags test contratos_ingest_dag 2026-01-01
```

---

## 3. Mapa do repositório

```
airflow/
  dags/
    dag_selector                   # allowlist do que este deployment carrega (ADR-0005)
    .airflowignore                 # dbt/ e superset/ fora da varredura do processor
    data_ingest/<sistema>/         # {entidade}_ingest_dag.py — compartilhadas entre órgãos
    data_ingest/<sistema>/<orgao>/ # {entidade}_{orgao}_ingest_dag.py — específica de um órgão
    data_transform/<orgao>/        # {orgao}_transform_dag.py — DbtDag via Cosmos (ADR-0018);
                                   # duckdb_{orgao}_transform_dag.py — nós com tag duckdb (ADR-0022)
    data_publish/<orgao>/          # {orgao}_publish_dag.py — importa bundles no Superset (ADR-0019)
    data_report/<orgao>/           # {relatorio}_{orgao}_report_dag.py — via relatorio_dag_factory
    dbt/gov_bricks/                # pacote base: macros compartilhados (chave_conformada, schema_medallion)
    dbt/<sistema>/                 # pacote dbt do sistema: Bronze (source) + Silver
    dbt/<orgao>/                   # projeto dbt do órgão: importa pacotes, materializa Gold
    dbt/profiles.yml               # profile único, tudo via env vars
    superset/<orgao>/              # bundles exportados do Superset + acesso.yml GERADO
    homologation/                  # vazio hoje (ver seção 9)
  helpers/                         # utilitários Python: landing_zone, batching, dag_discovery...
  plugins/                         # clientes de fonte e factories: cliente_compras_gov, cliente_storage...
catalogo/
  chaves.yml                       # chaves conformadas e pontes (ADR-0017)
  sistemas/<sistema>.yml           # entidades de cada sistema e as chaves que expõem
  acesso.yml                       # níveis de acesso (ADR-0020)
  publicacao/<orgao>.yml           # o que o órgão publica e quem consome (ADR-0019)
scripts/modelagem/                 # python -m scripts.modelagem {validar,sync,mapa,gerar}
scripts/publicacao/                # python -m scripts.publicacao {validar,sync,matriz,plano}
docker/                            # Dockerfile (airflow:3.2.2-python3.11), compose, init do Postgres
docs/adr/                          # ADR-0000 a ADR-0022
tests/unit/                        # roda no CI; importa DAGs via DagBag
tests/integration/                 # marcador `integration`; exige compose
```

`PYTHONPATH` inclui `airflow/`, `airflow/helpers` e `airflow/plugins` (Makefile,
pyproject, compose). Por isso as DAGs importam `from batching import ...` e
`from landing_zone import ...` sem prefixo de pacote. Mantenha esse padrão.

---

## 4. Regras que o CI ou os testes aplicam

Estas não são preferências. Violar qualquer uma quebra `make lint` ou
`make test`.

### Nomenclatura (ADR-0007, 0008, 0009, 0010, 0018, 0019)

- Só `a-z`, `0-9` e `_` em nomes de pasta, arquivo, `dag_id`, modelo, schema
  e tabela. Nada de hífen, acento ou maiúscula.
- `dag_id` **igual** ao nome do arquivo sem `.py`. Sufixos fixos:
  `_ingest_dag`, `_transform_dag`, `_publish_dag`, `_report_dag`.
- Toda DAG tem `description`, `default_args["owner"]` e tags no formato
  `dimensao:valor` com dimensão em `{sistema, orgao, camada, dominio}`.
  `sistema:` é obrigatória; `orgao:` obrigatória quando a DAG é de um órgão.
- DAG de transformação fica **sempre** em `data_transform/<orgao>/` e o nome
  termina em `{orgao}_transform_dag`.
- Modelos dbt vivem em `models/{bronze|silver|gold}/<escopo>/{entidade}.sql`.
  O schema de destino é derivado da pasta pelo macro `schema_medallion`:
  `silver/contratacoes/x.sql` → `002_slv_contratacoes.x`. Não use `+schema`
  no `dbt_project.yml` sem justificativa.

### Metadados dbt (ADR-0013)

Todo modelo em `schema.yml` tem `description`, `meta.owner` (time no
vocabulário do CODEOWNERS, nunca pessoa), `meta.sistema_origem` e
`meta.classificacao` em `{publico, interno, pessoal, pessoal_sensivel}`.
Colunas de Silver e Gold têm `description` e `meta.classificacao`. Coluna
sem classificação é tratada como `pessoal`, e a validação de publicação
reprova bundle que exponha coluna acima do nível do consumidor.

### Artefatos gerados — nunca edite à mão

| Arquivo | Regenerado por | Quando |
|---|---|---|
| `requirements.txt` | `make requirements` | depois de mexer em `pyproject.toml` |
| `airflow/dags/dbt/gov_bricks/macros/chaves_geradas.sql` | `make catalogo-sync` | depois de editar `catalogo/chaves.yml` |
| `airflow/dags/dbt/<sistema>/models/bronze/sources.yml` | `make modelo ARGS="--camada bronze --sistema X"` | depois de editar `catalogo/sistemas/X.yml` |
| `airflow/dags/superset/<orgao>/acesso.yml` | `make publicacao-sync` | depois de editar `catalogo/publicacao/<orgao>.yml` ou `catalogo/acesso.yml` |

O CI compara o artefato com o catálogo e reprova se divergirem. `sqlfmt`
ignora `chaves_geradas.sql` de propósito (`pyproject.toml`).

### Ingestão agnóstica de motor (ADR-0011, ADR-0021)

- **Nenhuma DAG de ingestão importa cliente de banco.** Ela grava com
  `landing_zone.write_raw(sistema, entidade, registros, primary_key=[...])`
  e lê o que já foi ingerido com `distinct_raw_values` / `distinct_raw_rows`.
  O backend (`RAW_BACKEND=object_storage|warehouse`) é resolvido lá dentro.
- O import de `cliente_postgres` dentro de `landing_zone.py` é **local à
  função** de propósito: um deployment `object_storage` pode não ter driver
  de banco instalado. Não suba esse import para o topo do módulo.
- A connection do warehouse é `postgres_dw` (destino analítico), não
  `postgres_default` (banco de apoio do Airflow). Confundir as duas foi o
  bug que motivou o ADR-0021.

### Fan-out particionado (`airflow/helpers/batching.py`)

Nunca faça `.expand()` sobre uma lista que pode ter milhares de elementos.
O Airflow cria uma task instance por elemento e estoura
`core.max_map_length` (1024), derrubando o scheduler. Use:

- `chunked(lista, BLOCK_SIZE)` para listas já materializadas (órgãos, itens),
  e itere dentro da task;
- `page_starts(total_paginas, BLOCK_SIZE)` para fontes paginadas;
- `block_offsets(total, BLOCK_SIZE)` para fatiar sequências por offset;
- `limit_local(lista, "INGEST_MAX_ORGAOS", "órgãos")` antes do `chunked`
  quando a lista vem da raw — trunca só no compose local, onde a variável
  está definida em `local.env`.

Modelo de referência: `airflow/dags/data_ingest/compras_gov/contratos_ingest_dag.py`.

### Sem credencial nem dado pessoal versionado

Senhas, tokens e endereços de e-mail ficam em Variables/Connections do
Airflow ou em variáveis de ambiente. A URI do Superset no `.env` tem a senha
mascarada e recebe `SUPERSET_DW_PASSWORD` por fora. Destinatários de
relatório ficam na Variable declarada em `destinatarios_variavel`.

---

## 5. Receitas para tarefas comuns

### Nova DAG de ingestão em um sistema já catalogado

1. Crie `airflow/dags/data_ingest/<sistema>/{entidade}_ingest_dag.py`
   copiando uma DAG vizinha. Paginada: copie `orgao_ingest_dag.py`. Dirigida
   por entidade da raw: copie `contratos_ingest_dag.py`.
2. `dag_id` = nome do arquivo. Tags `sistema:<sistema>` e `dominio:<x>`.
   `default_args` com `owner` e `queue`.
3. Grave com `write_raw(...)` e declare `primary_key` (vira upsert no
   backend `warehouse`).
4. Se a DAG deriva o intervalo de `context`, use o padrão tolerante a
   execuções manuais do `_get_intervalo` em `contratos_ingest_dag.py`.
   **Importante:** em trigger manual no Airflow 3, `data_interval_start`/
   `data_interval_end` podem faltar do `context` — e `dag_run.logical_date`
   *também* pode vir `None` nesses casos. O fallback precisa ir até
   `dag_run.run_after`, que é o único campo confirmado presente em toda
   run, manual ou agendada:
   ```python
   dag_run = context["dag_run"]
   fallback = dag_run.logical_date or dag_run.run_after
   data_interval_start = context.get("data_interval_start") or fallback
   data_interval_end = context.get("data_interval_end") or fallback
   ```
   Parar o fallback em `logical_date` sem chegar a `run_after` reproduz o
   `KeyError` original
5. Adicione a entidade em `catalogo/sistemas/<sistema>.yml` (com
   `dag:` e `chave_primaria:`), rode `make modelo ARGS="--camada bronze
   --sistema <sistema>"` para regenerar o `sources.yml`, e
   `make catalogo-validar`.
6. **Atualize o teste de contagem**: `tests/unit/test_data_ingest_compras_gov_dags.py`
   assere `len(dagbag.dags) == 23` para o `compras_gov`. Novo sistema pede um
   arquivo de teste novo no mesmo molde.
7. Se o deployment local deve carregar a DAG, confira que a pasta está no
   `airflow/dags/dag_selector` (pasta de sistema **não** inclui subpasta de
   órgão; cada uma precisa da própria linha).

### Novo modelo dbt

```bash
make modelo ARGS="--camada silver --sistema compras_gov --entidade contratos"
make modelo ARGS="--camada gold --orgao mgi --produto contratacoes --entidade x \
                  --cruzar compras_gov.contratos --cruzar compras_gov.uasg"
```

O gerador escreve o `.sql` com as chaves conformadas via
`gov_bricks.chave_conformada(...)` e a deduplicação por `dt_ingest`, e
acrescenta o bloco ao `schema.yml`. Depois:

- Troque todo `PREENCHER:` por descrição real. O CI de publicação lê esses
  metadados.
- Adicione as colunas de negócio à mão. O gerador só emite as chaves.
- Nunca `SELECT *` em modelo final. Nunca cruze sistemas por coluna crua,
  sempre pela chave conformada.
- `uv run sqlfmt airflow/dags/dbt` antes de commitar.
- O gerador não sobrescreve arquivo existente sem `--forcar`.
- Gold só cruza por ponte com `confiabilidade: total` ou `parcial`;
  `indicio` é proibido em Gold (ADR-0017).

Rodar dbt à mão a partir da pasta do projeto:

```bash
cd airflow/dags/dbt/mgi
DBT_PROFILES_DIR=.. DBT_HOST=localhost dbt deps && dbt build --select +contratos_por_orgao
```

### Novo domínio lendo a raw em Parquet (DuckDB)

A ARP é o modelo. Nenhuma DAG de transformação muda: a
`duckdb_<orgao>_transform_dag` seleciona `tag:duckdb` e deriva o agendamento
dos arquivos de Bronze.

1. Bronze: `dbt/<sistema>/models/bronze/<entidade>.sql` contendo só
   `{{ gov_bricks.bronze_parquet() }}`. O nome do arquivo **é** a entidade da
   raw (`<bucket>/<sistema>/<entidade>/**/*.parquet`). Documente em
   `models/bronze/schema.yml`.
2. Ingestão: na DAG da entidade, `@task(outlets=[asset_raw(SISTEMA, ENTIDADE)])`
   na task que roda depois de todos os lotes (ver `validate` da
   `arp_ingest_dag`).
3. Catálogo: `origem_atual: 001_bnz_<sistema>.<entidade>` e
   `make modelo ARGS="--camada bronze --sistema <sistema>"`, que move a
   entidade para o source `<sistema>_bronze`.
4. Silver: `models/silver/<dominio>/`, com a pasta listada sob `silver:` no
   `dbt_project.yml` do pacote com `+tags: ["duckdb"]`. Na Silver inicial
   (só renomeia para snake_case), declare `gov_bricks.mesma_contagem` e
   `gov_bricks.mesmo_conteudo_em_snake_case` com `comparar_com: ref("<entidade>")`.
5. Testes: uma amostra da entidade em `AMOSTRAS` de
   `tests/unit/test_fluxo_bronze_parquet_dbt.py`. Os demais testes percorrem
   os arquivos e já cobrem o domínio novo.

### Novo sistema estruturante

1. `cp catalogo/sistemas/_template.yml catalogo/sistemas/<sistema>.yml`.
   Nome do arquivo = campo `sistema` = pasta de DAG = escopo do schema Bronze.
2. Se surgir chave que não existe em `chaves.yml`, adicione lá primeiro e
   rode `make catalogo-sync`.
3. `make catalogo-validar && make mapa`.
4. Pacote dbt em `airflow/dags/dbt/<sistema>/` com `dbt_project.yml`,
   `packages.yml` apontando para `gov_bricks` via
   `{{ env_var('GOV_BRICKS_DBT_DIR', '..') }}` e o macro
   `generate_schema_name.sql` copiado de `compras_gov/macros/`.
5. `make modelo ARGS="--camada bronze --sistema <sistema>"` gera o `sources.yml`.

### Novo órgão

1. `airflow/dags/dbt/<orgao>/` com `packages.yml` importando `gov_bricks` e
   os pacotes de sistema que consome; modelos em `models/gold/<produto>/`.
2. `airflow/dags/data_transform/<orgao>/{orgao}_transform_dag.py` copiando o
   do MGI e trocando `ORGAO`.
3. `catalogo/publicacao/<orgao>.yml` a partir de `_template.yml`;
   `make publicacao-sync` gera `airflow/dags/superset/<orgao>/acesso.yml`.
4. DAG de publicação e de relatório copiando as do MGI.
5. Linhas no `dag_selector` e no `.github/CODEOWNERS` (há blocos comentados
   prontos para ipea, mir, mcid, minc; a última regra que casa vence).

### Nova dashboard ou relatório

- Dashboard: construa no Superset local (`make superset`), exporte o bundle,
  descompacte em `airflow/dags/superset/<orgao>/<bundle>/`, declare em
  `catalogo/publicacao/<orgao>.yml` (`dashboards:` e `datasets:`). A URI da
  database no bundle é placeholder; a DAG substitui por `SUPERSET_DW_URI`.
- Relatório: arquivo `data_report/<orgao>/{relatorio}_{orgao}_report_dag.py`
  chamando `build_relatorio_dag(...)`. A consulta **precisa** conter
  `{recorte}` na cláusula `where`; sem a marca a factory recusa a DAG.
  Declare em `relatorios:` do catálogo com o mesmo `dag:`.
- `make publicacao-validar` reprova: coluna acima do nível, coluna sem
  classificação, dataset no bundle que não está no catálogo, DAG de relatório
  não declarada, plano fora de sincronia.

### Novo ADR

Copie `docs/adr/template.md` para `NNNN-titulo-kebab.md` (próximo número:
0023), preencha **todas** as seções, inclusive Alternativas e Tradeoffs com
`[Alto/Médio/Baixo impacto]` e uma Avaliação explícita. Um ADR só cita ADRs
de número menor. Adicione a linha ao índice do `docs/adr/README.md`. ADR
aceito não é editado retroativamente: crie um novo e marque o antigo como
substituído.

---

## 6. Como os testes funcionam

- `tests/unit/conftest.py` coloca `helpers/` e `plugins/` no `sys.path` e
  **substitui por `MagicMock`** os módulos `airflow.decorators`,
  `airflow.sensors*`, `postgres_helpers`, `schedule_loader`, `s3fs` e `adlfs`.
  Se um teste seu precisa do comportamento real de um desses, ele não vai
  ter: use `monkeypatch`/`patch` explicitamente ou reveja o stub.
- `tests/conftest.py` define `AIRFLOW__COSMOS__ENABLE_CACHE=False` porque o
  Cosmos tentaria gravar uma Variable no metadata DB durante o import da DAG
  de transformação.
- As DAGs são testadas por **import via `DagBag`** por pasta
  (`test_data_ingest_compras_gov_dags.py`, `test_data_transform_dags.py`,
  `test_data_publish_report_dags.py`). Isso já cobre: erro de import,
  `dag_id` = arquivo, sufixo, tags, description, owner. Carregar a DAG de
  transformação roda `dbt ls` de verdade, então um `packages.yml` quebrado
  falha aqui, não só em produção.
- Testes de helper importam o módulo direto (`import batching`,
  `import landing_zone`) e usam `monkeypatch.setenv("RAW_BACKEND", ...)`
  para exercitar os dois backends.
- Cobertura mede só `airflow/plugins` e `airflow/helpers`.
- Integração (`tests/integration/`, marcador `integration`) conecta **pelo
  host** em MinIO e Postgres do compose, lendo as portas do `.env`. Não roda
  no CI.

Ao mudar comportamento de DAG, ajuste ou acrescente teste no arquivo
correspondente. Ao criar helper novo em `airflow/helpers/`, crie
`tests/unit/test_<helper>.py`.

---

## 7. Armadilhas conhecidas (aprendidas na prática)

Cada item abaixo já quebrou algo. Os comentários no código explicam mais.

**Airflow 3 e descoberta de DAGs**

- `dag_discovery.might_contain_selected_dag` é o callable de
  `core.might_contain_dag_callable`. Ele **deve** chamar
  `might_contain_dag_via_default_heuristic`, nunca `might_contain_dag`: a
  segunda é o despachante que aponta para ele mesmo, e a recursão derruba o
  processor sem carregar DAG nenhuma.
- `dbt/` e `superset/` estão no `.airflowignore` por necessidade: `dbt deps`
  cria symlinks de `dbt/<orgao>/dbt_packages/<pacote>` para dentro de
  `dags/`, e a varredura detecta loop recursivo e aborta **tudo**.
- Sem `dag_selector`, tudo é incluído (default compatível). Com ele, só o que
  está listado. Pasta de sistema não inclui subpasta de órgão.
- Importe `dag` e `task` de `airflow.sdk`, não de `airflow.decorators`.

**Compose local**

- `AIRFLOW__API_AUTH__JWT_SECRET` precisa estar fixo: sem ele scheduler e
  api-server do `standalone` sorteiam segredos diferentes e toda task morre
  com "Invalid auth token" sem escrever log. `airflow dags test` não revela
  isso porque roda num processo só.
- A senha da UI é semeada em `simple_auth_manager_passwords.json.generated`
  antes do `standalone`; sem isso o SimpleAuthManager sorteia senha a cada
  recriação do container.
- `hostname: airflow` no serviço é o que faz a UI achar logs de execuções
  anteriores.
- Logs de task ficam em volume nomeado; `up --build` não os apaga.
- Portas 5432/9000/9001 costumam colidir com outro projeto na máquina.
  Sobrescreva `POSTGRES_HOST_PORT`, `MINIO_HOST_PORT`,
  `MINIO_CONSOLE_HOST_PORT` no `.env` (não versionado). Dentro da rede do
  compose nada muda.
- `RAW_BACKEND=warehouse` no `local.env` porque o compose só tem Postgres, que
  não lê Parquet. O padrão do framework é `object_storage`.
- `INGEST_MAX_ORGAOS=10` no `local.env` para as DAGs por órgão terminarem em
  minutos. Ausente em homologação e produção.
- `docker/postgres/init.sh` cria os bancos `airflow` e `data_warehouse` e o
  papel `postgres_dw`. Testado em 2026-09-02: a conexão do Superset local
  usa `postgres_dw`, e o `init.sh` só dá `GRANT` em `public` — schemas
  criados depois (ex. `compras_gov`) ficam sem permissão a cada `down -v`.
  Ao ver `permission denied for schema <x>` no Superset, rode
  `GRANT USAGE ON SCHEMA <x> TO postgres_dw; GRANT SELECT ON ALL TABLES
  IN SCHEMA <x> TO postgres_dw;` direto no Postgres.

**dbt e Cosmos**

- `GOV_BRICKS_DBT_DIR` é injetado pela `DbtDag` porque o Cosmos copia o
  projeto para um diretório temporário antes do `dbt deps`; de lá, o
  `../gov_bricks` do `packages.yml` apontaria para fora do monorepo.
- `generate_schema_name` precisa existir em **cada projeto raiz**
  (`compras_gov/macros/`, `mgi/macros/`) delegando para
  `gov_bricks.schema_medallion`; dbt só honra a sobrescrita no projeto raiz.
- Macros compartilhados são sempre qualificados: `gov_bricks.chave_conformada`,
  não `chave_conformada`.
- `test_behavior=AFTER_EACH`: o teste roda logo após cada modelo, então um
  defeito na Silver para o ramo antes da Gold.
- Teste que dá `ref()` em dois modelos (ex. `gov_bricks.mesma_contagem`
  entre Silver e Bronze) entra, no `AFTER_EACH`, no `.test` do primeiro
  modelo, antes de o segundo ser reconstruído. Na primeira execução a tabela
  nem existe; nas seguintes, o teste compara com a versão anterior. Use
  `RenderConfig(should_detach_multiple_parents_tests=True)`, que move esses
  testes para uma task própria depois de todos os pais.
- O cache do Cosmos (Variable `cosmos_cache__<dag_id>`) **não** é invalidado
  por mudanças nos pacotes que o projeto importa (`compras_gov`, por exemplo).
  Ao testar uma mudança no pacote, use `AIRFLOW__COSMOS__ENABLE_CACHE=False`
  ou apague a Variable; senão a DAG pode mostrar o grafo antigo.
- Nós com tag dbt `duckdb` leem Parquet com funções do DuckDB e só rodam no
  target `prod_duckdb`, na `duckdb_<orgao>_transform_dag`. A DAG do projeto
  inteiro os exclui com `exclude=["tag:duckdb"]`, e um `dbt build` manual no
  `dev` precisa do mesmo `--exclude tag:duckdb`. Eles ficam habilitados, e
  não com `enabled=false`, para que um modelo no PostgreSQL possa dar `ref()`
  neles: o `+database` aponta para a mesma tabela física nos dois targets.
  Testes genéricos não herdam a tag da pasta, mas a seleção indireta do dbt
  os inclui e exclui junto com o modelo.
- No `prod_duckdb`, não declare `database` no profile: o dbt-duckdb o deriva
  do `path` e recusa os dois divergindo. As DAGs usam
  `DUCKDB_GOVHUB_PATH=:memory:`, porque um arquivo teria lock de escrita
  disputado por tasks paralelas.
- `target/`, `dbt_packages/`, `logs/`, `package-lock.yml` e `.user.yml`
  dentro de `airflow/dags/dbt/` são artefatos locais ignorados pelo git. Se
  aparecerem no `git status`, algo mudou no `.gitignore`.

**Ingestão**

- `write_raw` usa `pl.DataFrame(records, infer_schema_length=None)`: com
  amostragem, coluna nula nas primeiras linhas viraria `Null` e perderia os
  valores seguintes.
- `write_raw` devolve `None` e loga aviso quando a lista está vazia; não
  trate isso como erro.
- A raw em `object_storage` é append-only: `read_raw` concatena **todos** os
  Parquets da entidade. Deduplicar é papel da Silver (`row_number() ...
  order by dt_ingest desc`).
- DAGs dirigidas pela raw (contratos, contratos_item, pesquisa de preço,
  arp_detalhes) dependem de `orgao_ingest_dag` / `arp_item_ingest_dag` já
  terem rodado. Elas falham com mensagem explícita quando a entidade não
  está na raw.
- `ClienteComprasGov.iter_pages` dorme 1s entre páginas e continua na página
  seguinte em caso de erro; `max_active_tis_per_dag` nas tasks de fetch
  limita a concorrência contra a API pública.
- Nunca rode `airflow dags test <dag_id>` (CLI) enquanto a mesma DAG tem uma
  run ativa disparada pela UI ou por `airflow dags trigger`: as duas
  competem pelo mesmo `dag_run` no metadata DB e podem gerar
  `psycopg2.errors.DeadlockDetected`, travando a execução até o timeout.
  Antes de testar, confira runs presas com
  `airflow dags list-runs <dag_id> --state running`; para limpar uma run
  zumbi, `UPDATE dag_run SET state='failed' WHERE state='running'` direto
  no Postgres de metadados (banco `airflow`) é o caminho mais confiável.

---

## 8. Estilo e processo

- **Formatação**: `black` com linha de 90, `ruff` com 120 (regras E, F, W,
  C90 sem C901), `sqlfmt` polyglot com 120, `ty` para tipos. `make format`
  resolve quase tudo.
- **Idioma**: o CONTRIBUTING pede código e comentários em inglês, mas a
  prática consolidada no código novo (helpers, DAGs de publicação, zona raw,
  scripts, testes, ADRs) é **português**, com termos técnicos em inglês
  quando forem mais claros. Siga o módulo que você está editando; em código
  novo, português com docstrings que explicam o *porquê* e citam o ADR.
  Documentação, issues, PRs e commits sempre em português.
- **Docstrings e comentários** explicam decisões e armadilhas, não repetem o
  código. É a convenção mais forte deste repositório; veja `landing_zone.py`,
  `dag_discovery.py` e `docker-compose.yml`.
- **Commits**: Conventional Commits em português, imperativo, minúsculas,
  sem ponto final. Escopo é a pasta ou o tema: `feat(ingestao): ...`,
  `fix(dag-discovery): ...`, `test(relatorio): ...`, `docs: ...`. O corpo
  explica o porquê e, quando for o caso, o número de antes e depois.
- **Branches**: `<tipo>/<descricao-curta>` ou `<issue>-<tipo>-<descricao>`.
  Merge por *merge commit*, branch apagada depois.
- **PR**: use o template; título Conventional Commits; label `team:*` para
  acionar a revisão por domínio; mínimo 1 aprovação; CODEOWNERS decide quem
  aprova cada pasta.
- **Dependências**: entram no `pyproject.toml`, seguidas de `uv lock` e
  `make requirements`. Discuta em issue antes de adicionar pacote novo.
- **Novos utilitários compartilhados** vão para `airflow/helpers/` (lógica
  pura) ou `airflow/plugins/` (clientes de fonte, factories), com teste
  unitário e menção na tabela do README.

---

## 9. Estado atual e frentes em aberto

O que está feito e o que não está, para você não redescobrir.

**Feito e funcionando**

- 23 DAGs de ingestão do Compras.gov.br gravando na zona raw com fan-out
  particionado.
- Pacote dbt `compras_gov` (Silver: contratos, contratos_item, fornecedor,
  orgao, uasg) e projeto `mgi` (Gold: contratos_por_orgao), executados por
  Cosmos.
- Catálogo de chaves conformadas com gerador de modelos, mapa de cruzamento
  e validação no CI.
- Publicação: bundle de dashboard do MGI, plano de acesso gerado, DAG de
  publicação e DAG de relatório mensal com recorte por órgão.
- Ambiente local completo no compose, incluindo Superset em perfil opcional.

**Em aberto (registrado nos ADRs e no código)**

- `airflow/dags/homologation/` está vazio. O ADR-0021 reposiciona a DAG de
  homologação como **validação** da raw (sem cópia), usando
  `homologation_helpers.py` e `homologation_flow.py`, que existem mas não
  são usados por DAG nenhuma.
- O `sources.yml` gerado só tem a forma de tabela (backend `warehouse`). A
  forma de source externo para `object_storage` nunca foi executada por
  engine nenhum.
- ADR-0015 (Extractor via Strategy) e ADR-0016 (tipagem de Parquet) estão
  propostos e **não implementados**. As DAGs de ingestão seguem o padrão
  TaskFlow direto, não o de `Extractor`.
- Todos os ADRs estão com status `Proposto`; nenhum foi formalmente
  `Aceito`.
- Há 14 marcadores `PREENCHER:` nos `schema.yml` de Silver do `compras_gov`
  e no SQL da Gold `contratos_por_orgao` (`grep -rn PREENCHER airflow/dags/dbt`).
- A ARP é o primeiro domínio no modelo Parquet → DuckDB → PostgreSQL (receita
  na seção 5): Bronze `001_bnz_compras_gov.arp` e Silver
  `002_slv_arp.silver_compras_gov_arp`, na `duckdb_mgi_transform_dag`. As
  entidades `arp_item` e `arp_detalhes` ainda não são modeladas. O padrão é o
  ADR-0022, que registra como pendente a conversão de
  `gov_bricks.bronze_parquet()` para `adapter.dispatch()` (exceção ao ADR-0011).
- `catalogo/publicacao/mgi.yml`: o consumidor `mgi` tem `abrangencia: total`
  e `codigo_orgao_verificado: false`. `make lint` emite dois avisos por isso;
  não são erro, mas alguém precisa conferir o código 48000 contra a tabela
  de órgãos ingerida.
- Enforcement automático dos metadados por camada (ADR-0013) só existe para o
  que a publicação valida; não há validação genérica de `schema.yml` no CI.
- CODEOWNERS tem as linhas de órgão (ipea, mir, mcid, minc) comentadas até os
  projetos migrarem.
- Renomear o schema raw para o padrão `001_bnz_<sistema>` do ADR-0010 quando
  os consumidores de BI puderem ser coordenados (hoje é `compras_gov`).

---

## 10. Como o repositório chegou aqui

Ordem cronológica dos PRs em `main`, para dar contexto ao que você vai
encontrar no `git log` e no `git blame`.

| PR | O que entrou |
|---|---|
| (pré-#1) | ADRs 0000 a 0016: por que ADRs, Airflow, dbt, framework compartilhado, monorepo, dag_selector, medallion, nomenclaturas, motor agnóstico, object storage, metadados, OpenMetadata, Extractor, Parquet tipado. |
| #1 | Port da base de código anterior: 23 DAGs do Compras.gov.br gravando direto no Postgres, plugins e helpers, compose, CI, CONTRIBUTING e protocolo de PR. |
| #2 | `dag_selector` implementado em `dag_discovery.py` (ADR-0005). |
| #3 | Upgrade para Airflow 3: Dockerfile, Makefile, dependências. |
| #6 | Migração das DAGs do MGI para a estrutura nova de pastas. |
| #7 | Catálogo de chaves conformadas, gerador de modelos dbt e mapa de cruzamento (ADR-0017); DAG de transformação via Cosmos (ADR-0018); correção da recursão do `might_contain_dag` no Airflow 3; import de `airflow.sdk`. |
| #9 | Publicação: dashboards versionadas, relatórios por órgão e níveis de acesso (ADR-0019, ADR-0020). Zona raw com backend intercambiável e ingestão sem cliente de banco (ADR-0021). |
| #11 | Destravar a execução das DAGs no compose: JWT secret, senha do SimpleAuthManager, hostname fixo, logs em volume, portas remapeáveis, `init.sh` do Postgres. |
| #13 | Fan-out particionado (`batching.py`, `INGEST_MAX_ORGAOS`) após o `max_map_length` estourar com ~10 mil órgãos; `_get_intervalo` tolerante a execução manual (fallback até `dag_run.run_after`). |

Quando estiver em dúvida sobre *por que* algo é como é, a ordem de consulta
é: comentário no código → ADR citado → mensagem do commit (`git log -p` nos
arquivos do PR). As mensagens de commit deste repositório costumam explicar
o problema e os números que o motivaram.
