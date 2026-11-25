# Architecture Decision Records (ADRs)

Este diretório guarda as decisões arquiteturais e técnicas do `data-framework`,
o framework de engenharia de dados compartilhado por projetos de dados públicos
no Brasil (GovHub).

## Por que ADRs

Este repositório é herdado por múltiplos projetos. Decisões tomadas aqui se
propagam para todos os consumidores do framework, muitas vezes sem que os
times consumidores participem da discussão original. Um ADR registra o
contexto, as alternativas avaliadas e o racional de cada decisão, permitindo
que qualquer time — presente ou futuro — entenda *por que* o framework é como
é, não apenas *o que* ele faz.

Veja o racional completo e os benefícios deste processo em
[ADR-0000](./0000-por-que-adrs.md), baseado no
[guia de ADRs da AWS](https://docs.aws.amazon.com/pt_br/prescriptive-guidance/latest/architectural-decision-records/adr-process.html).

## Quando criar uma ADR

Crie uma ADR quando a decisão afetar arquitetura, manutenção, operação ou
evolução do GovHub de forma que alguém possa precisar entender o motivo no
futuro. A pergunta prática é: se uma pessoa nova entrar no projeto daqui a
seis meses, ela precisará saber por que escolhemos esse caminho?

Use ADRs para decisões como:

- escolha ou troca de ferramentas centrais, como orquestrador, ferramenta de
  transformação, motor de consulta, catálogo ou armazenamento;
- mudança em padrões compartilhados de DAGs, dbt, schemas, camadas, catálogo,
  publicação ou governança;
- definição de arquitetura que afeta mais de um time, órgão, produto de dados
  ou repositório;
- decisão que cria restrições, tradeoffs, custos de migração ou dependências
  relevantes;
- substituição de uma decisão arquitetural registrada em ADR anterior.

Não crie ADR para:

- correções pontuais de bug;
- mudanças pequenas de texto, layout ou nomenclatura sem impacto arquitetural;
- tarefas operacionais sem decisão técnica relevante;
- decisões já cobertas por ADR existente, quando basta referenciar a decisão
  atual.

Se houver dúvida, abra a discussão no PR ou na issue. Se a decisão precisar
explicar contexto, alternativas e tradeoffs, ela provavelmente merece uma ADR.

## Onde registrar

- Decisões do framework compartilhado ficam neste diretório: `docs/adr/`.
- Decisões específicas de outro repositório devem ficar no `docs/adr/` do
  próprio repositório, seguindo este padrão como referência.
- Quando uma mudança em outro repositório depender de uma decisão registrada
  aqui, referencie a ADR no PR, na issue ou na documentação afetada.

## Como usar no fluxo de trabalho

1. Identifique durante uma issue, PR ou discussão que existe uma decisão
   arquitetural a registrar.
2. Copie [`template.md`](./template.md) para um novo arquivo numerado.
3. Preencha o contexto, a decisão, as alternativas consideradas, os tradeoffs
   e as consequências.
4. Abra ou atualize o PR com a ADR junto da mudança relacionada.
5. Peça revisão de pelo menos uma pessoa do time responsável.
6. Depois da aprovação, mantenha a ADR como histórico da decisão. Se a decisão
   mudar no futuro, crie uma nova ADR e marque a anterior como substituída.

## Convenções

- **Numeração**: sequencial, com 4 dígitos.
  Ex.: `0003-govhub-como-framework-compartilhado-de-dados.md`.
- **Nome de arquivo**: `NNNN-titulo-em-kebab-case.md`.
- **Imutabilidade**: um ADR aceito não é editado retroativamente para mudar a
  decisão. Se a decisão muda, crie um novo ADR e marque o antigo como
  `Superseded by ADR-XXXX`.
- **Idioma**: português, salvo quando o termo técnico for mais claro em
  inglês (ex.: nomes de ferramentas, siglas do ecossistema de dados).
- **Template**: todo ADR novo parte de [`template.md`](./template.md).

## Ciclo de vida (Status)

| Status | Significado |
|---|---|
| `Backlog` | Necessidade de decisão identificada e número reservado, mas ADR ainda não escrito. |
| `Proposto` | Em discussão, ainda não decidido. |
| `Aceito` | Decisão tomada e revisada por pelo menos 1 par. |
| `Rejeitado` | Proposta avaliada e descartada. |
| `Substituído por ADR-XXXX` | Decisão superada por um ADR mais recente. |
| `Depreciado` | Decisão não é mais relevante (contexto desapareceu). |

## Como propor um novo ADR

1. Copie `template.md` para `NNNN-titulo-em-kebab-case.md` (próximo número
   disponível) — ou, se já existir um arquivo stub com status `Backlog` para
   o tema, parta dele em vez de criar um novo número.
2. Preencha todas as seções — especialmente **Alternativas** e **Tradeoffs**,
   que são obrigatórias mesmo quando a decisão parece óbvia.
3. Abra um PR. O ADR deve ser revisado como qualquer mudança de arquitetura.
4. Ao ser aceito, atualize o status e adicione o ADR ao índice abaixo.

### Arquivos `Backlog`

Um ADR com status `Backlog` reserva um número para um tema já identificado
como carente de decisão, mas ainda não escrito — útil para mapear a dívida
de documentação sem bloquear a numeração de ADRs que já estão prontos. Ele
contém só título, status e um resumo de escopo; quem for escrevê-lo de fato
substitui o corpo pelo conteúdo de `template.md`, preenchido normalmente.

## Mensagem sugerida para divulgação

> Pessoal, vamos usar ADRs para registrar decisões técnicas e arquiteturais
> importantes do GovHub. A ideia não é documentar qualquer ajuste pequeno, mas
> deixar registrado o contexto, as alternativas e os tradeoffs de decisões que
> afetam arquitetura, manutenção, operação ou evolução do projeto. O padrão está
> documentado em `docs/adr/README.md`, e novos registros devem partir do
> `docs/adr/template.md`.

## Índice

| ADR | Título | Status |
|---|---|---|
| [0000](./0000-por-que-adrs.md) | Por que ADRs | Proposto |
| [0001](./0001-airflow-como-orquestrador-de-fluxos-de-dados.md) | Apache Airflow como orquestrador de fluxos de dados | Proposto |
| [0002](./0002-dbt-como-ferramenta-de-transformacao-de-dados.md) | dbt como ferramenta de transformação de dados | Proposto |
| [0003](./0003-govhub-como-framework-compartilhado-de-dados.md) | GovHub como um framework compartilhado de dados | Proposto |
| [0004](./0004-monorepo-como-estrategia-de-organizacao-de-codigo.md) | Monorepo como estratégia de organização de código | Proposto |
| [0005](./0005-selecao-de-dags-por-dag-selector-antes-do-parsing.md) | Seleção de DAGs por arquivo `dag_selector` antes do parsing | Proposto |
| [0006](./0006-arquitetura-medallion.md) | Arquitetura medallion (bronze/silver/gold) | Proposto |
| [0007](./0007-nomenclatura-pastas-dags-ingestao.md) | Padrão de nomenclatura das pastas/arquivos de DAGs de ingestão | Proposto |
| [0008](./0008-padrao-uso-tags-nomenclatura.md) | Padrão de uso de tags/nomenclatura no Airflow | Proposto |
| [0009](./0009-nomenclatura-pastas-arquivos-dbt.md) | Padrão de nomenclatura das pastas/arquivos de projetos dbt | Proposto |
| [0010](./0010-nomenclatura-schemas-tabelas-bronze-silver-gold.md) | Padrão de nomenclatura de schemas e tabelas bronze/silver/gold | Proposto |
| [0011](./0011-arquitetura-agnostica-motor-processamento.md) | Arquitetura agnóstica a motor de processamento de dados | Proposto |
| [0012](./0012-ingestao-object-storage-vs-database.md) | Ingestão em object storage em vez de banco diretamente | Proposto |
| [0013](./0013-padrao-documentacao-metadados-tabelas.md) | Padrão de documentação de metadados de tabelas | Proposto |
| [0014](./0014-padrao-documentacao-dags-dbt-openmetadata.md) | Padrão de documentação de DAGs/dbt para envio ao OpenMetadata | Proposto |
| [0015](./0015-estrategia-extractors-via-strategy.md) | Estratégia de `Extractor` via Strategy para DAGs | Proposto |
| [0016](./0016-tipagem-parquet-staging-bronze.md) | Tipagem de Parquet — promover Staging (MinIO) → Bronze | Proposto |
| [0017](./0017-chaves-conformadas-cruzamento-sistemas-estruturantes.md) | Chaves conformadas e catálogo de cruzamento entre sistemas estruturantes | Proposto |
| [0018](./0018-nomenclatura-execucao-dags-transformacao.md) | Padrão de nomenclatura e execução das DAGs de transformação | Proposto |
| [0019](./0019-publicacao-dashboards-relatorios.md) | Publicação de produtos de dados — dashboards versionadas e relatórios periódicos | Proposto |
| [0020](./0020-niveis-acesso-consumo-dados.md) | Níveis de acesso derivados da classificação de sensibilidade | Proposto |
| [0021](./0021-zona-raw-com-backend-intercambiavel.md) | Zona raw com backend intercambiável e Bronze lendo da raw | Proposto |
| [0022](./0022-transformacao-parquet-com-duckdb-e-assets.md) | Transformação da raw em Parquet com DuckDB e agendamento por Assets | Proposto |
