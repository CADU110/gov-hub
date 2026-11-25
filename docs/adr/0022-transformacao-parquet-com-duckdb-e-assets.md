# ADR-0022: Transformação da raw em Parquet com DuckDB e agendamento por Assets

- **Status**: Proposto
- **Data**: 2026-10-27
- **Autores**: -
- **Revisores**: -

## Introdução ao problema

O [ADR-0012](./0012-ingestao-object-storage-vs-database.md) levou a ingestão
para object storage, e o
[ADR-0021](./0021-zona-raw-com-backend-intercambiavel.md) tornou a zona raw
intercambiável entre Parquet (`object_storage`) e tabela (`warehouse`), com a
Bronze declarada como `source` dbt sobre ela. O ADR-0021 deixou um risco
registrado: no backend `object_storage`, a leitura da raw pelo dbt nunca tinha
sido executada por engine nenhum.

A ARP (Atas de Registro de Preço, do Compras.gov.br) é o primeiro domínio a
percorrer esse caminho de verdade. A raw dela está em Parquet, mas o destino
analítico do órgão continua sendo o PostgreSQL, que é onde o Superset lê. O
PostgreSQL não lê Parquet, então a Bronze precisa de outro motor. A primeira
versão usou DuckDB (`read_parquet`) num modelo dbt do pacote `compras_gov` e
expôs três problemas:

- **A DAG do projeto passou a falhar todo dia.** A
  `<orgao>_transform_dag` do
  [ADR-0018](./0018-nomenclatura-execucao-dags-transformacao.md) executa o
  grafo inteiro do projeto do órgão no target PostgreSQL. O modelo novo entrou
  nesse grafo e quebrou no primeiro nó (`column "union_by_name" does not
  exist`), deixando a Silver e os testes do ramo como `SKIP`.
- **Trocar o target da DAG inteira não resolve.** No target DuckDB, os sources
  relacionais e a Gold não declaram `database` e resolvem para o arquivo
  DuckDB local, onde essas tabelas não existem. Na revisão do PR, o
  `+contratos_por_orgao` nesse target terminou com `ERROR=4`.
- **Ingestão e transformação rodavam no mesmo horário.** A Bronze lê todos os
  Parquets disponíveis no momento em que roda; disparada por cron junto com a
  ingestão, ela pode capturar só parte dos lotes do dia. Os testes que comparam
  Bronze e Silver passariam mesmo assim.

A decisão precisa ser tomada agora porque a ARP é só o primeiro domínio: os
próximos que tiverem a raw em Parquet vão repetir o mesmo caminho. Sem um
padrão, cada um reinventa DAG, agendamento e testes.

As restrições conhecidas: o PostgreSQL continua sendo o destino analítico; o
código compartilhado deve ser agnóstico de motor
([ADR-0011](./0011-arquitetura-agnostica-motor-processamento.md)); a execução
é por Cosmos, uma task por nó dbt (ADR-0018); e o Airflow 3 já oferece
agendamento por Assets.

## Decisão

**Modelos dbt que leem a raw em Parquet são marcados com a tag `duckdb` e
executados por uma DAG de transformação separada, no target DuckDB, que grava
no PostgreSQL anexado. Essa DAG é acionada pelos Assets da raw que as DAGs de
ingestão emitem ao terminar.**

### DuckDB lê o Parquet e grava no PostgreSQL anexado

O profile compartilhado (`airflow/dags/dbt/profiles.yml`) ganha o target
`prod_duckdb`. Nele, o DuckDB lê o object storage pela extensão `httpfs` e
anexa o PostgreSQL do órgão pela extensão `postgres`, com o alias
`data_warehouse`. Os modelos desse ramo declaram `database` igual a esse alias,
então as tabelas ficam fisicamente no PostgreSQL. Nada de útil fica no banco
DuckDB local, e por isso a DAG o abre em memória (`:memory:`): um arquivo teria
lock de escrita disputado por tasks paralelas.

A Bronze em Parquet é um modelo dbt materializado como tabela, no schema
`001_bnz_<sistema>` do
[ADR-0010](./0010-nomenclatura-schemas-tabelas-bronze-silver-gold.md). O SQL
específico do DuckDB fica num único macro, `gov_bricks.bronze_parquet()`, que
lê `s3://<bucket>/<sistema>/<entidade>/**/*.parquet` com `union_by_name`. O
caminho segue uma convenção: o pacote do modelo é o sistema, e o nome do
arquivo é a entidade, o mesmo par usado pela ingestão. Uma Bronze nova é só o
arquivo `models/bronze/<entidade>.sql` chamando o macro. A Silver é construída
sobre ela no schema do próprio domínio, como qualquer outra.

### A tag `duckdb` separa os dois ramos

A tag é aplicada por pasta no `dbt_project.yml` do pacote: a pasta `bronze/`
inteira (a Bronze relacional continua como `source`) e cada pasta de Silver
construída sobre uma Bronze em Parquet.

- A DAG do projeto, no PostgreSQL, usa `exclude=["tag:duckdb"]`.
- A DAG DuckDB usa `select=["tag:duckdb"]`.
- Os testes desses modelos entram e saem junto com eles, pela seleção indireta
  do dbt.

Os nós continuam habilitados, e não com `enabled=false`. Um modelo
desabilitado não pode ser alvo de `ref()`, e uma Gold que roda no PostgreSQL
vai precisar ler uma Silver feita pelo DuckDB. O `+database` resolve para
`data_warehouse` no DuckDB e para `target.database` no PostgreSQL, e os dois
apontam para a mesma tabela física.

### Uma DAG DuckDB por órgão

A DAG fica em `data_transform/<orgao>/duckdb_<orgao>_transform_dag.py`, no
formato `{escopo}_{orgao}_transform_dag` do ADR-0018, com o recorte expresso
pelo seletor `tag:duckdb`. Ela usa o mesmo projeto dbt do órgão, Cosmos e
`TestBehavior.AFTER_EACH`. Também usa `should_detach_multiple_parents_tests`:
os testes que comparam Silver e Bronze têm dois pais, e sem essa opção o Cosmos
os rodaria junto com o teste da Bronze, antes de a Silver ser reconstruída.

Um domínio novo nesse modelo entra sem mudar o arquivo da DAG.

### Assets da raw acionam a DAG DuckDB

- `landing_zone.asset_raw(sistema, entidade)` devolve o Asset
  `raw.<sistema>.<entidade>`. O nome não depende do `RAW_BACKEND`, então trocar
  o backend não quebra o agendamento.
- A DAG de ingestão declara esse Asset como `outlet` da task que só roda
  depois de todos os lotes. Se um lote falha, a task não roda e o evento não é
  emitido.
- A DAG DuckDB deriva a lista de Assets dos arquivos de Bronze dos pacotes que o
  projeto do órgão importa, pela mesma convenção do macro.

O agendamento combina esses Assets com **`AssetAny`**: qualquer entidade que
termine de ser ingerida dispara uma reconstrução. Assim, as entidades
concluídas são processadas sem esperar uma ingestão que falhou. Se outra
ingestão ainda estiver em andamento quando a execução começar, a Bronze dela
sai com os lotes que já existiam, e a conclusão daquela ingestão dispara uma
execução posterior, que a completa.

**`max_active_runs=1`** impede duas reconstruções simultâneas das mesmas
tabelas. Cada execução reconstrói todas as tabelas do ramo, e duas em paralelo
só disputariam o mesmo destino. Um evento que chega durante uma execução não se
perde: resulta numa execução seguinte.

### Relação com ADRs anteriores

- **ADR-0021**: para as entidades em Parquet, a Bronze deixa de ser um
  `source` sobre a raw e passa a ser uma tabela materializada pelo dbt. É a
  mitigação que o próprio ADR-0021 previa para a leitura de arquivos a cada
  execução. O catálogo registra isso com `origem_atual:
  001_bnz_<sistema>.<entidade>`, e o `sources.yml` gerado declara essas tabelas
  no source `<sistema>_bronze`.
- **ADR-0012**: para essas entidades, a DAG DuckDB faz o papel do estágio de
  promoção da landing zone para a Bronze, em dbt, e não numa DAG de
  homologação em Python.
- **ADR-0011**: este ADR abre uma exceção aos itens 2 e 6. O `read_parquet` é
  específico do DuckDB, e o modelo que o usa fica num pacote de sistema
  compartilhado (`dbt/<sistema>/`). A especificidade fica confinada a um macro
  e a nós identificados pela tag, mas o macro ainda não usa
  `adapter.dispatch()`.
- **ADR-0018**: o `{escopo}` da DAG DuckDB é um recorte por motor, e não um
  produto ou um domínio como o ADR-0018 exemplifica. Continua sendo um
  seletor dbt, como ele exige.

## Alternativas consideradas

### Alternativa A: desabilitar o ramo fora do DuckDB e rodar à mão

- Descrição: `enabled=target.type == "duckdb"` nos modelos e testes do ramo;
  a DAG do projeto deixa de vê-los, e a Bronze e a Silver rodam com `dbt
  build --target prod_duckdb` manual.
- Prós: muda poucas linhas, já foi testada e destrava a DAG do projeto na
  hora.
- Contras: um modelo desabilitado não pode receber `ref()`, então a primeira
  Gold no PostgreSQL que dependesse de um domínio em Parquet quebraria o parse
  do target PostgreSQL. E o ramo continuaria sem execução agendada.

### Alternativa B: rodar o projeto inteiro no target DuckDB

- Descrição: trocar o target da `<orgao>_transform_dag` para `prod_duckdb`,
  mantendo uma DAG só.
- Prós: um grafo, uma DAG e uma ordem de execução derivada inteiramente dos
  `ref()`.
- Contras: os sources relacionais e a Gold não declaram `database` e resolvem
  para o banco DuckDB local; o teste da revisão terminou com `ERROR=4`. Exige migrar o
  projeto inteiro de uma vez e revisar o SQL existente, escrito para o
  PostgreSQL.

### Alternativa C: uma DAG por domínio (`<dominio>_<orgao>_transform_dag`)

- Descrição: cada domínio em Parquet ganha a própria DAG, com seletor e Asset
  próprios. Foi a forma da primeira correção, com a `arp_mgi_transform_dag`.
- Prós: segue o ADR-0018 ao pé da letra (escopo = domínio); cada domínio é
  agendado só pela própria ingestão e falha isolado.
- Contras: um arquivo de DAG por domínio, com configuração repetida e
  específica (nome, seletor, entidades). Foi descartada para que um domínio
  novo não exigisse código novo de orquestração.

### Alternativa D: agendar por horário, depois da janela de ingestão

- Descrição: um cron posterior às DAGs de ingestão, como a DAG do projeto já
  faz.
- Prós: não introduz Assets; segue o padrão que já existe.
- Contras: a dependência fica implícita no relógio. Uma ingestão que atrase ou
  falhe não impede a transformação de ler lotes parciais, e os testes Bronze ×
  Silver não detectam isso.

### Alternativa E: `AssetAll`, esperar todas as entidades

- Descrição: a DAG DuckDB só roda quando todas as entidades tiverem um evento
  novo.
- Prós: uma execução por ciclo, sempre sobre um conjunto completo.
- Contras: uma ingestão que falhou deixa todos os domínios parados, e
  entidades com cadências diferentes (diária, mensal) passam a andar no ritmo
  da mais lenta.

### Alternativa F: manter a Bronze como source externo, sem materializar

- Descrição: seguir o ADR-0021 à risca e deixar a Silver ler os Parquets
  diretamente.
- Prós: sem cópia entre a raw e a Bronze.
- Contras: a Bronze não ficaria no PostgreSQL, onde estão os consumidores e as
  permissões. Cada leitura da Silver voltaria ao object storage. E um modelo no
  PostgreSQL não teria como dar `ref()` nela.

### Não fazer nada / manter status quo

A Bronze da ARP no grafo da DAG do projeto faz a DAG falhar todo dia depois do
merge, e a ARP não chega ao PostgreSQL por nenhum caminho agendado. Não é
aceitável.

## Tradeoffs

### Vantagens

- **[Alto impacto]** A DAG do projeto volta a ter só nós que o PostgreSQL
  executa, e uma falha no ramo DuckDB não a afeta.
- **[Alto impacto]** A transformação de uma entidade só começa depois que a
  ingestão dela gravou todos os lotes, sem depender de janela de horário.
- **[Médio impacto]** Um domínio novo em Parquet entra só com arquivos dbt (a
  Bronze de uma linha, a tag na pasta da Silver) e o `outlet` na ingestão. A
  DAG e os testes genéricos não mudam.
- **[Médio impacto]** Um modelo no PostgreSQL pode dar `ref()` numa tabela
  feita pelo DuckDB. Verificado no compose: um modelo no target PostgreSQL leu,
  via `ref()`, a Silver construída pelo DuckDB.
- **[Médio impacto]** A leitura da raw `object_storage` por um engine, risco
  ativo do ADR-0021, passa a ser executada e testada.
- **[Baixo impacto]** O DuckDB em memória dispensa arquivo local, volume e
  coordenação de lock.

### Desvantagens

- **[Alto impacto]** O pacote compartilhado passa a ter SQL de um motor
  específico, contra os itens 2 e 6 do ADR-0011. Um deployment sem DuckDB não
  materializa essas Bronzes.
- **[Médio impacto]** Cada órgão passa a ter duas DAGs de transformação, e a
  ordem entre elas não é derivada dos `ref()`. Uma Gold no PostgreSQL sobre uma
  Silver do DuckDB depende de horário até haver um Asset entre as duas DAGs.
- **[Médio impacto]** Cada execução reconstrói todas as tabelas do ramo e lê
  todo o histórico de Parquets. Com `AssetAny`, isso acontece uma vez por
  entidade ingerida, e o custo cresce com o volume e com o número de domínios.
- **[Médio impacto]** Uma execução disparada no meio da ingestão de outra
  entidade publica, até a execução seguinte, uma Bronze parcial dessa outra
  entidade.
- **[Baixo impacto]** A Bronze materializada duplica o dado da raw, o que o
  ADR-0021 queria evitar.
- **[Baixo impacto]** A convenção "arquivo da Bronze = entidade da raw" e "a
  pasta `bronze/` só tem Parquet" é verificada por testes do repositório, não
  pelo dbt nem pelo Airflow.

### Avaliação

Os ganhos superam os custos. O problema que motivou a decisão era operacional
e imediato: a DAG do projeto falharia todo dia depois do merge, e o domínio
novo não tinha execução agendada. A separação por tag resolve isso sem
congelar o futuro, porque mantém o `ref()` entre os ramos, e o agendamento por
Assets troca uma dependência implícita no relógio por uma explícita no fim da
ingestão.

`AssetAny` foi escolhido conscientemente em vez de `AssetAll`. O custo dele,
reconstruções repetidas e Bronze parcial transitória, é menor que o de deixar
todos os domínios parados por uma ingestão que falhou. A execução seguinte
sempre converge para o estado completo.

Permanecem como riscos ativos:

- a exceção ao ADR-0011, mitigada por confinar o SQL de motor num macro e por
  marcar os nós com a tag, mas ainda sem `adapter.dispatch()`;
- o custo de reconstrução integral, que deve ser medido quando houver mais de
  um domínio ou volume maior.

## Consequências

- **Positivas**: a DAG do projeto deixa de quebrar com modelos que o PostgreSQL
  não executa. Os domínios em Parquet passam a ter execução agendada pelo fim
  da ingestão, e o padrão vale para os próximos sem código novo de
  orquestração.
- **Negativas**: o pacote compartilhado ganha dependência de motor; cada órgão
  passa a ter duas DAGs de transformação sem ordem automática entre elas; e o
  custo por execução cresce com o histórico de Parquets.
- **Ações decorrentes**:
  - Converter `gov_bricks.bronze_parquet()` para `adapter.dispatch()` quando um
    segundo motor precisar ler a raw, como o ADR-0011 pede.
  - Declarar `database` nos sources relacionais e na Gold antes de qualquer
    tentativa de rodar o projeto inteiro no DuckDB.
  - Ordenar a DAG do projeto depois da DAG DuckDB, por Asset, quando a
    primeira Gold depender de uma Silver feita no DuckDB.
  - Avaliar a leitura incremental dos Parquets quando o volume crescer.
  - Fazer o gerador de modelos (`make modelo`) emitir a Bronze em Parquet e a
    Silver inicial no formato deste ADR.
  - Pré-instalar as extensões `httpfs` e `postgres` do DuckDB na imagem, para
    o worker não baixá-las em tempo de execução.
  - Ao copiar a DAG DuckDB para outro órgão, trocar apenas `ORGAO`, como já se
    faz com a DAG do projeto.

## Referências

- [ADR-0010 — Padrão de nomenclatura de schemas e tabelas bronze/silver/gold](./0010-nomenclatura-schemas-tabelas-bronze-silver-gold.md)
- [ADR-0011 — Arquitetura agnóstica a motor de processamento de dados](./0011-arquitetura-agnostica-motor-processamento.md)
- [ADR-0012 — Ingestão em object storage em vez de banco diretamente](./0012-ingestao-object-storage-vs-database.md)
- [ADR-0018 — Padrão de nomenclatura e execução das DAGs de transformação](./0018-nomenclatura-execucao-dags-transformacao.md)
- [ADR-0021 — Zona raw com backend intercambiável e Bronze lendo da raw](./0021-zona-raw-com-backend-intercambiavel.md)
- [dbt-duckdb — adapter, `attach` e extensões](https://github.com/duckdb/dbt-duckdb)
- [Astronomer Cosmos — documentação](https://astronomer.github.io/astronomer-cosmos/)
- [Apache Airflow — agendamento por Assets](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html)
