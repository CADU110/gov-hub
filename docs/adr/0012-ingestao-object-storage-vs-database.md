# ADR-0012: Ingestão em object storage em vez de banco diretamente

- **Status**: Proposto
- **Data**: 2026-08-07
- **Autores**: -
- **Revisores**: -

## Introdução ao problema

O [ADR-0006](./0006-arquitetura-medallion.md) define o contrato de qualidade
das camadas Bronze/Silver/Gold, mas deixa explicitamente em aberto **onde
cada camada é fisicamente persistida** e se existe algum estágio físico
antes da Bronze. A prática corrente nos projetos consumidores é a mais
direta possível: a DAG de ingestão lê a fonte (API, banco, arquivo, e-mail)
e escreve o resultado direto em uma tabela do banco analítico, dentro da
mesma execução.

Essa prática gera quatro problemas recorrentes:

- **Reprocessar exige rebuscar a fonte.** Se a carga falha na metade, se o
  parsing estava errado, ou se um bug de tipagem só é descoberto semanas
  depois, não existe cópia do que a fonte devolveu — a única saída é chamar
  a fonte de novo. Muitas fontes públicas não permitem isso: janelas de
  consulta limitadas, dados que mudam retroativamente, relatórios enviados
  por e-mail que não podem ser reemitidos, APIs com rate limit agressivo.
  Isso contradiz na prática a promessa de auditabilidade da Bronze imutável
  do ADR-0006, que passa a depender da fonte externa continuar disponível.
- **Falha parcial deixa o banco em estado intermediário.** Extrair e
  carregar na mesma transação lógica significa que um erro no meio da carga
  já sujou a tabela de destino, e a retentativa precisa lidar com estado
  parcial em vez de partir de um insumo estável.
- **Acoplamento entre ingestão e motor.** Escrever direto no banco faz cada
  DAG de ingestão conhecer o destino analítico, seu dialeto e seus tipos —
  exatamente o acoplamento que o
  [ADR-0011](./0011-arquitetura-agnostica-motor-processamento.md) restringe.
  Um órgão que troque o banco precisaria mexer nas DAGs, não só na
  configuração.
- **O banco vira área de staging.** Dados brutos, com o schema irregular da
  fonte, ocupam espaço e I/O do banco analítico — recurso caro e limitado
  nos ambientes on-premise da maioria dos órgãos — antes mesmo de terem
  passado por qualquer validação.

Essa decisão precisa ser tomada agora porque ela define o **formato do
contrato entre a DAG de ingestão e tudo o que vem depois dela**. Cada DAG
escrita antes dessa definição é uma DAG a migrar depois, e o número de DAGs
cresce a cada fonte adicionada.

## Decisão

A ingestão do `data-framework` escreve em **object storage**, não no banco.
O fluxo de uma fonte até a Bronze passa a ter dois estágios desacoplados:

1. **DAG de ingestão** (`*_ingest_dag.py`,
   [ADR-0007](./0007-nomenclatura-pastas-dags-ingestao.md)): extrai da fonte
   e escreve o resultado em **arquivos Parquet** em uma *landing zone* de
   object storage. Não conhece o banco de destino.
2. **DAG de homologação/promoção**: lê os arquivos da landing zone, aplica
   verificações de qualidade e materializa a camada Bronze no destino
   analítico do órgão. É o único estágio que conhece o banco.

Elementos concretos da decisão:

- **Formato**: Parquet — colunar, comprimido, com schema embutido no
  arquivo, e legível por todos os motores considerados alvo pelo ADR-0011.
  Não CSV/JSON solto, que não carregam tipagem.
- **Convenção de caminho**, particionada por data de execução para tornar
  reprocessamento e retenção operações de prefixo:

  ```text
  {bucket}/{sistema_origem}/{entidade}/{ano}/{mes}/{dia}/{run_id}--lote-{lote_id}.parquet
  ```

  `{sistema_origem}` e `{entidade}` são os mesmos nomes usados pelas DAGs de
  ingestão (ADR-0007) e pelos schemas Bronze
  ([ADR-0010](./0010-nomenclatura-schemas-tabelas-bronze-silver-gold.md));
  `{run_id}` é o identificador de execução do Airflow, o que torna cada
  escrita rastreável até a execução que a produziu.
- **Backend intercambiável**: o acesso ao object storage passa por uma
  abstração de sistema de arquivos (`fsspec`), com o backend selecionado por
  variável de ambiente (`minio` on-premise, `s3` ou `adls` em nuvem). MinIO
  e S3 compartilham o mesmo protocolo, então não há caminho de código
  distinto entre on-prem e nuvem — coerente com a exigência de
  agnosticismo do ADR-0011.
- **Imutabilidade**: arquivos da landing zone não são sobrescritos nem
  editados. Uma reexecução gera um novo arquivo (novo `run_id`), preservando
  o histórico do que cada execução devolveu.
- **A landing zone não é a Bronze.** Ela é um estágio físico *anterior* à
  Bronze — a possibilidade prevista, e deixada opcional, pelo ADR-0006. Os
  consumidores (dashboards, análises) continuam sem acessá-la, assim como não
  acessam a Bronze.

Este ADR **não** define política de retenção da landing zone nem trata de
promover a própria landing zone a Bronze com contrato de tipos — decisões
complementares que dependem desta e serão registradas separadamente.

## Alternativas consideradas

### Alternativa A: Escrever direto no banco (status quo / não fazer nada)

- Descrição: a DAG de ingestão carrega o resultado da extração diretamente
  em uma tabela do banco analítico, sem estágio intermediário.
- Prós: um estágio a menos por fonte; menos infraestrutura para operar
  (nenhum object storage); o dado fica consultável via SQL imediatamente
  após a ingestão.
- Contras: é exatamente o cenário que produz os quatro problemas da
  introdução — reprocessamento dependente da fonte externa, estado parcial
  em falhas, acoplamento entre DAG e motor, e uso do banco como área de
  staging. Nenhum deles é resolvido incrementalmente: todos decorrem de não
  existir um insumo estável entre extrair e carregar.

### Alternativa B: Estágio intermediário em disco local do worker

- Descrição: a DAG grava arquivos em disco local (ou volume compartilhado)
  do worker do Airflow, e a etapa seguinte lê dali.
- Prós: desacopla extração de carga sem exigir object storage; simples de
  operar em ambiente de desenvolvimento.
- Contras: o insumo deixa de ser durável — em execução distribuída, a etapa
  seguinte pode cair em outro worker, e o dado é perdido em recriação de
  contêiner. Isso reintroduz a dependência da fonte para reprocessar, que é
  o problema central. Também não escala em retenção: disco de worker não é
  dimensionado para guardar histórico de ingestões.

### Alternativa C: Adotar um table format (Iceberg/Delta/Hudi) já na ingestão

- Descrição: a landing zone é escrita já como tabela transacional em um
  table format, com catálogo, evolução de schema e viagem no tempo.
- Prós: resolve, além do desacoplamento, evolução de schema, snapshots e
  atomicidade de escrita; é a evolução natural desta decisão.
- Contras: exige catálogo e um motor com suporte ao formato em **todos** os
  deployments, incluindo os ambientes on-premise mínimos (Airflow +
  PostgreSQL) que são o cenário mais comum hoje — o que colide com a
  exigência de agnosticismo do ADR-0011. É a alternativa mais forte
  tecnicamente, mas cara demais para o ponto de maturidade atual; a decisão
  aqui não a impede no futuro, já que Parquet é a base física desses
  formatos.

## Tradeoffs

### Vantagens

- **[Alto impacto]** Reprocessar deixa de depender da fonte externa: o dado
  bruto já está persistido e versionado por execução, o que efetivamente
  entrega a auditabilidade que o ADR-0006 promete para a Bronze.
- **[Alto impacto]** Extração e carga viram etapas independentes — uma falha
  na carga não exige refazer a extração, e nem deixa o banco em estado
  parcial.
- **[Médio impacto]** A DAG de ingestão deixa de conhecer o banco de
  destino, cumprindo na prática a fronteira "mover vs. transformar" do
  ADR-0011 e permitindo que a mesma DAG sirva órgãos com motores diferentes.
- **[Médio impacto]** Tira do banco analítico a carga de servir como área de
  staging de dados brutos — relevante nos ambientes on-premise, onde esse
  recurso é o mais escasso.
- **[Médio impacto]** Parquet carrega tipagem e compressão, eliminando a
  reinferência de tipos a cada leitura que formatos texto exigem.
- **[Baixo impacto]** Particionamento por data no caminho torna retenção e
  reprocessamento de janela operações de prefixo, sem varrer o acervo.

### Desvantagens

- **[Alto impacto]** Adiciona um componente de infraestrutura a operar em
  todo deployment (MinIO on-premise): mais uma superfície para provisionar,
  monitorar, fazer backup e controlar acesso — em órgãos onde a equipe de
  infraestrutura já é o gargalo.
- **[Médio impacto]** Todo dado passa a existir em duas cópias (landing zone
  + Bronze), aumentando custo de armazenamento — e, sem política de
  retenção definida (que este ADR não define), esse custo cresce
  indefinidamente.
- **[Médio impacto]** Aumenta a latência entre "a fonte respondeu" e "o dado
  está consultável", já que a promoção para Bronze passa a ser uma execução
  separada, com seu próprio agendamento e suas próprias falhas.
- **[Médio impacto]** Cada fonte passa a exigir duas DAGs em vez de uma,
  aumentando o número de objetos a escrever, revisar e operar — custo que
  se soma ao overhead de pipelines já apontado pelo ADR-0006.
- **[Baixo impacto]** Arquivo Parquet não é inspecionável com as mesmas
  ferramentas de um banco: investigar "o que a fonte devolveu naquele dia"
  passa a exigir ferramental próprio, não uma query SQL.

### Avaliação

Os ganhos superam os custos. O problema decisivo é o primeiro: sem um
insumo persistido, a camada Bronze do ADR-0006 só é auditável enquanto a
fonte externa continuar disponível e reproduzível — condição que boa parte
das fontes públicas brasileiras não satisfaz. Nenhuma das alternativas mais
baratas resolve isso (a Alternativa B só o mascara), e o custo principal —
operar mais um componente — é pago uma vez por deployment, enquanto o custo
de não ter a cópia é pago a cada incidente de dados.

Permanecem como **riscos ativos**: (i) o crescimento não limitado da landing
zone, até que exista política de retenção; e (ii) a duplicação de DAGs por
fonte, que aumenta o custo de manutenção proporcionalmente ao número de
fontes e só é mitigável por fatoração dos padrões comuns em helpers
compartilhados.

## Consequências

- **Positivas**: existe um insumo estável, imutável e rastreável por
  execução entre a fonte e o banco; reprocessamento e correção de bugs de
  parsing deixam de depender da fonte; DAGs de ingestão tornam-se portáveis
  entre órgãos com motores diferentes; o banco analítico deixa de ser área
  de staging.
- **Negativas**: object storage passa a ser componente obrigatório de todo
  deployment; o dado existe em duas cópias, com custo de armazenamento
  crescente enquanto não houver retenção definida; o número de DAGs por
  fonte dobra; a latência até o dado consultável aumenta.
- **Ações decorrentes**:
  - Prover, no ambiente de desenvolvimento local do framework, um object
    storage compatível (MinIO) já configurado, para que o padrão seja o
    caminho de menor atrito.
  - Fatorar em helpers compartilhados a escrita/leitura da landing zone e
    as verificações de qualidade da promoção, de modo que a segunda DAG por
    fonte seja majoritariamente declarativa.
  - Definir política de retenção da landing zone (por camada de fonte e por
    sensibilidade do dado, considerando LGPD) em decisão complementar.
  - Definir os critérios de qualidade mínimos que bloqueiam a promoção de
    landing zone para Bronze, alinhados ao contrato de camadas do ADR-0006.
  - Prever período de transição em que a escrita no banco durante a
    ingestão continue possível, atrás de um sinalizador de configuração
    desligado por padrão, para migrar DAGs existentes sem interrupção.

## Referências

- [ADR-0006 — Arquitetura medallion (bronze/silver/gold)](./0006-arquitetura-medallion.md)
- [ADR-0007 — Padrão de nomenclatura das pastas/arquivos de DAGs de ingestão](./0007-nomenclatura-pastas-dags-ingestao.md)
- [ADR-0010 — Padrão de nomenclatura de schemas e tabelas bronze/silver/gold](./0010-nomenclatura-schemas-tabelas-bronze-silver-gold.md)
- [ADR-0011 — Arquitetura agnóstica a motor de processamento de dados](./0011-arquitetura-agnostica-motor-processamento.md)
- [Apache Parquet — especificação do formato](https://parquet.apache.org/docs/)
- [fsspec — Filesystem interfaces for Python](https://filesystem-spec.readthedocs.io/)
- [Estrutura de ADRs do repositório](./README.md)
