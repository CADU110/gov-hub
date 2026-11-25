# ADR-0021: Zona raw com backend intercambiável e Bronze lendo da raw

- **Status**: Proposto
- **Data**: 2026-08-25
- **Autores**: -
- **Revisores**: -

## Introdução ao problema

O [ADR-0012](./0012-ingestao-object-storage-vs-database.md) decidiu que a
ingestão escreve em object storage e que um segundo estágio — uma DAG de
homologação — lê esses arquivos e **materializa a Bronze** no banco do órgão,
sendo "o único estágio que conhece o banco".

Dessa decisão, só a primeira metade foi construída, e nem ela: as 23 DAGs de
ingestão do Compras.gov.br escrevem **direto no Postgres**, via
`ClientPostgresDB`, e nunca tocam a landing zone. Os helpers do segundo estágio
existem (`landing_zone.py`, `homologation_flow.py`), mas `dags/homologation/`
está vazio e nenhuma DAG os usa.

O resultado são três problemas que só aparecem quando se tenta rodar a cadeia
inteira:

- **A ingestão conhece o motor.** Toda DAG de ingestão importa um cliente de
  Postgres, o que o [ADR-0011](./0011-arquitetura-agnostica-motor-processamento.md)
  proíbe explicitamente para código compartilhado: uma DAG "não usa um SDK de
  motor (`pyspark`, cliente de warehouse) que assumiria um cluster específico".
  Trocar o destino analítico hoje significa editar 23 arquivos.
- **A ingestão e a transformação não se encontram.** As DAGs gravam no banco
  `postgres`, pela connection `postgres_default`, enquanto o dbt lê de
  `data_warehouse`. Os dois lados nunca apontaram para o mesmo lugar — o que
  passou despercebido porque a cadeia completa nunca tinha sido executada.
- **A cópia para a Bronze é um estágio a mais sem função clara.** Copiar o dado
  da landing zone para uma tabela idêntica no banco duplica armazenamento e
  cria uma segunda verdade sobre "o que foi ingerido", sem transformar nada: a
  Bronze é, por definição do
  [ADR-0006](./0006-arquitetura-medallion.md), fiel à fonte.

Há ainda uma restrição de realidade: nem todo deployment do framework é um
datalake. Um órgão que roda só Postgres não tem engine capaz de ler Parquet, e
para ele "landing zone em object storage" significaria manter MinIO só para
depois copiar tudo para o banco de qualquer forma.

## Decisão

**A zona raw é uma camada com forma física intercambiável, e a Bronze é um
`source` dbt sobre ela — sem cópia entre as duas.**

### A raw tem dois backends, escolhidos por configuração

A ingestão entrega o dado a "a zona raw" e não sabe qual é a forma dela. O
backend é selecionado por variável de ambiente `RAW_BACKEND`, no mesmo modelo
que o `STORAGE_BACKEND` já usa para object storage (ADR-0011, item 5):

| `RAW_BACKEND` | Forma física | Deployment típico |
|---|---|---|
| `object_storage` (padrão) | Parquet na convenção de caminho do ADR-0012: `{bucket}/{sistema}/{entidade}/{ano}/{mes}/{dia}/{run_id}--lote-{lote_id}.parquet` | Datalake — engine que lê arquivos (DuckDB, Trino, Spark, Athena) |
| `warehouse` | Tabela `raw_{entidade}` no schema do sistema, no banco analítico | Órgão que roda só um banco relacional |

O que **não** muda entre os dois: o vocabulário. `{sistema}` e `{entidade}` são
os mesmos nomes usados pelas DAGs de ingestão (ADR-0007), pelo catálogo
(ADR-0017) e pelos schemas do [ADR-0010](./0010-nomenclatura-schemas-tabelas-bronze-silver-gold.md).
Trocar o backend não renomeia nada.

### A ingestão não conhece o banco

Nenhuma DAG de ingestão importa cliente de banco. Ela chama uma única função da
zona raw, que carimba a coluna de ingestão e despacha para o backend
configurado. O conhecimento sobre motor fica confinado a esse despacho — que é
o ponto de extensão para um motor novo.

A connection usada pelo backend `warehouse` é a do **destino analítico**
(`postgres_dw`), não a do banco de apoio do Airflow. É o que faz ingestão e dbt
apontarem, finalmente, para o mesmo lugar.

### A Bronze lê da raw, em vez de copiá-la

A Bronze continua sendo declarada como `source` dbt (ADR-0017), e esse source
aponta para a raw:

- backend `warehouse`: o source é o schema do sistema, com `identifier`
  `raw_{entidade}` — que é exatamente o que o `sources.yml` gerado já faz hoje;
- backend `object_storage`: o source é externo, apontando para o prefixo de
  arquivos da entidade, na sintaxe do adapter em uso.

Não existe DAG de promoção copiando raw para Bronze. A camada Bronze do
ADR-0006 permanece como **contrato lógico** — dado fiel à fonte, imutável, não
acessível a consumidor final — e deixa de exigir uma materialização própria.

A DAG de homologação prevista pelo ADR-0012 continua fazendo sentido, mas com
outro papel: **validar** o que chegou na raw, não copiá-lo. Os helpers de
qualidade já escritos servem a esse propósito sem alteração.

## Alternativas consideradas

### Alternativa A: manter a cópia raw → Bronze do ADR-0012

- Descrição: ingestão escreve Parquet; uma DAG de promoção carrega tabelas
  Bronze no banco.
- Prós: a Bronze fica materializada no banco, com desempenho previsível de
  leitura para a Silver, independentemente do backend da raw.
- Contras: duplica o dado e cria duas verdades sobre "o que foi ingerido", sem
  transformação entre elas. Para um deployment de banco único — o caso do órgão
  que roda só Postgres — obriga a manter object storage apenas como estação de
  passagem. E acrescenta um estágio que pode falhar, atrasar ou divergir.

### Alternativa B: só trocar a connection das DAGs para o warehouse

- Descrição: manter a escrita direta no banco, apontando para `data_warehouse`.
- Prós: mudança de uma linha por DAG; nenhum conceito novo.
- Contras: resolve o sintoma e preserva a causa — as DAGs continuam importando
  um cliente de Postgres, contrariando o ADR-0011, e trocar de motor continua
  significando reescrever a ingestão inteira.

### Alternativa C: object storage sempre, sem backend alternativo

- Descrição: cumprir o ADR-0012 ao pé da letra e exigir engine que leia
  arquivos.
- Prós: uma forma física só, menos código, e a decisão original preservada.
- Contras: exclui do framework o órgão que roda apenas um banco relacional —
  que é a configuração mais comum entre os consumidores atuais — ou o obriga a
  operar MinIO e um engine de leitura de arquivos que ele não tem.

### Não fazer nada / manter status quo

O status quo é uma ingestão que grava em um banco que ninguém lê: as DAGs
escrevem em `postgres` e o dbt lê de `data_warehouse`. Qualquer execução real da
cadeia completa falha, e foi assim que o problema apareceu.

## Tradeoffs

### Vantagens

- **[Alto impacto]** A ingestão deixa de conhecer o motor: trocar o destino
  analítico passa a ser configuração, não reescrita de 23 DAGs — que é
  exatamente o que o ADR-0011 exige do código compartilhado.
- **[Alto impacto]** Ingestão e transformação passam a apontar para o mesmo
  lugar, e a cadeia fonte → Gold roda de ponta a ponta.
- **[Médio impacto]** O framework passa a servir tanto ao deployment de banco
  único quanto ao datalake, sem caminho de código distinto na ingestão.
- **[Médio impacto]** Some um estágio inteiro (a cópia raw → Bronze), com o
  armazenamento duplicado e o modo de falha que ele trazia.

### Desvantagens

- **[Alto impacto]** No backend `object_storage`, a Silver passa a ler arquivos
  a cada execução, sem uma tabela Bronze materializada no meio. Em volume
  grande e engine sem cache, isso custa mais que ler uma tabela — e a mitigação
  (materializar a Bronze como tabela no projeto dbt) fica a cargo do órgão.
- **[Médio impacto]** A declaração do source Bronze passa a depender do backend:
  o `sources.yml` gerado deixa de ser o mesmo arquivo em todo deployment.
- **[Médio impacto]** Dois backends é o dobro de superfície para testar, e só um
  deles (`warehouse`) é exercitado no compose local hoje.
- **[Baixo impacto]** O ADR-0012 fica parcialmente revisado sem ser substituído,
  o que exige ler os dois para entender o desenho completo.

### Avaliação

Os ganhos superam os custos. A desvantagem mais séria — leitura de arquivos sem
Bronze materializada — só incide no backend de datalake, onde o engine escolhido
é justamente o que sabe fazer isso bem; e continua disponível ao órgão
materializar a Bronze como modelo dbt se medir que precisa. Em troca, o
framework deixa de embutir na ingestão a suposição de qual banco existe do outro
lado, que é a premissa que o ADR-0011 tinha estabelecido e que a implementação
atual contraria.

Permanece como risco ativo a **leitura** da raw pelo dbt no backend
`object_storage`. A escrita e a leitura pela zona raw estão exercitadas contra
MinIO de verdade, mas o compose local roda Postgres, que não lê Parquet: o
`sources.yml` na forma de source externo ainda não foi executado por engine
nenhum.

## Consequências

- **Positivas**: a ingestão vira agnóstica de verdade, a cadeia completa passa a
  funcionar, e o órgão de banco único deixa de precisar de object storage.
- **Negativas**: o desenho da Bronze passa a variar por deployment, e o ADR-0012
  precisa ser lido junto deste para fazer sentido.
- **Ações decorrentes**:
  - Revisar o `sources.yml` gerado quando o primeiro deployment usar o backend
    `object_storage` — hoje o gerador emite apenas a forma de schema.
  - Reposicionar a DAG de homologação prevista pelo ADR-0012 como validação da
    raw, sem cópia, aproveitando os helpers de qualidade já existentes.
  - Renomear o schema raw para a nomenclatura do ADR-0010 quando os consumidores
    de BI puderem ser coordenados — a raw e a Bronze passam a ser o mesmo lugar
    físico, e o nome deveria refletir isso.
  - Avaliar, com volume real, se a Silver sobre arquivos exige materializar a
    Bronze no backend de datalake.

## Referências

- [ADR-0006 — Arquitetura medallion (bronze/silver/gold)](./0006-arquitetura-medallion.md)
- [ADR-0007 — Padrão de nomenclatura das pastas/arquivos de DAGs de ingestão](./0007-nomenclatura-pastas-dags-ingestao.md)
- [ADR-0010 — Padrão de nomenclatura de schemas e tabelas bronze/silver/gold](./0010-nomenclatura-schemas-tabelas-bronze-silver-gold.md)
- [ADR-0011 — Arquitetura agnóstica a motor de processamento de dados](./0011-arquitetura-agnostica-motor-processamento.md)
- [ADR-0012 — Ingestão em object storage em vez de banco diretamente](./0012-ingestao-object-storage-vs-database.md)
- [ADR-0017 — Chaves conformadas e catálogo de cruzamento entre sistemas estruturantes](./0017-chaves-conformadas-cruzamento-sistemas-estruturantes.md)
