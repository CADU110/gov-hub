# Ingestão do Contratos.gov.br — decisões operacionais

Registro das decisões que a fundação da ingestão (issue #15) precisa fixar antes
da primeira DAG, e da medição que as fundamenta. Não é um ADR: aqui não se
decide arquitetura, decide-se **como operar** a ingestão de um sistema dentro da
arquitetura que os ADRs já definiram. O que está aqui é o que as issues filhas
(#16 a #33) consomem como dado de entrada.

Levantamento e medições feitos em **2026-09-21**, contra a API de produção.

## Por que a varredura é o problema

A API aberta do Contratos.gov.br não tem paginação, não tem filtro por período e
não tem `updated_at` no cabeçalho do contrato. Os 18 endpoints que filtram por
`dt_alteracao_min/max` exigem JWT de usuário e respondem 401 sem token. A
consequência é que **não existe carga incremental**: cada execução varre tudo.

São 3.781 unidades gestoras e 599 órgãos. Cada UG devolve todos os seus
contratos numa resposta só — de 1 a 733 contratos, até 1,98 MB. Detalhar os
sub-recursos de todos os contratos de todas as UGs seria da ordem de milhões de
chamadas, o que está fora de questão; daí o escopo de detalhamento mais abaixo.

## Teste de carga

Feito com `scripts/contratos_gov/teste_carga.py`, amostra de 30 UGs sorteadas
com semente fixa (a mesma amostra em todos os níveis, senão a comparação mediria
o tamanho das UGs sorteadas em vez da concorrência):

```
uv run python -m scripts.contratos_gov.teste_carga --ugs 30 --concorrencias 1,2,4
```

| Concorrência | UGs | Erros | Total (s) | UG/s | p50 (s) | p95 (s) | máx (s) | MB |
|---|---|---|---|---|---|---|---|---|
| 1 | 30 | 0 | 87,8 | 0,34 | 0,69 | 22,34 | 22,49 | 13,1 |
| 2 | 30 | 0 | 59,7 | 0,50 | 0,71 | 22,05 | 34,00 | 13,1 |
| 4 | 30 | 0 | 31,7 | 0,95 | 0,80 | 20,26 | 23,03 | 13,1 |

Leitura: **zero erro em todos os níveis**, e a mediana por UG praticamente não se
move de 1 para 4 (0,69 s → 0,80 s). A API não deu sinal de degradação, e o ganho
de 1 para 4 é quase linear (2,8x). A distribuição é muito assimétrica — mediana
abaixo de 1 s e p95 acima de 20 s —, o que era esperado: poucas UGs concentram a
maior parte do volume.

### Decisão: concorrência 4

`max_active_tis_per_dag=4` nas DAGs que varrem por UG (#18, #19), mesmo valor já
usado nas DAGs do `compras_gov` contra a API pública. Extrapolando 0,95 UG/s, a
varredura completa das 3.781 UGs fica em **cerca de 1h10**, contra as 3h do
sequencial.

**Concorrência acima de 4 não foi medida.** Subir sem medir de novo é ir além da
evidência que existe: o teste cobriu 30 UGs de 3.781, e o comportamento da API
sob varredura completa continua não observado. A primeira varredura completa
deve ser acompanhada, e o resultado, registrado aqui.

## Horários

O `compras_gov` ocupa 01:00–07:00 (ARP às 06:00 e 07:00) e o
`mgi_transform_dag` roda às 06:00. As DAGs do `contratos_gov` ficam fora dessa
janela inteira, para não disputar rede com a ingestão existente nem atrasar a
transformação:

| DAG | Frequência | Horário |
|---|---|---|
| `unidade_contratante_ingest_dag` | diária | 22:00 |
| `orgao_contratante_ingest_dag` | diária | 22:10 |
| `contrato_ativo_ingest_dag` | semanal (sáb) | 08:00 |
| `contrato_inativo_ingest_dag` | semanal (sáb) | 10:00 |
| sub-recursos (`contrato_*`) | semanal (sáb) | a partir das 12:00, escalonados de 30 em 30 min, na ordem das fases da issue #15 |

As enumerações são diárias porque são baratas (uma chamada cada, poucos KB) e é
delas que sai a lista de UGs da varredura. Os cabeçalhos e os sub-recursos são
semanais porque não há como saber o que mudou: sem carga incremental, aumentar a
frequência multiplica o custo sem aumentar a informação.

O sábado é deliberado: a varredura de cabeçalhos leva mais de uma hora, os
sub-recursos vêm depois dela, e assim a cadeia inteira termina com folga antes da
janela do `compras_gov` no domingo de madrugada.

## Contratos inativos: UG vazia é a regra

Amostra de 12 UGs sorteadas (semente 1) contra
`/api/contrato/inativo/ug/{codigo}`, em **2026-10-07**, de um total de 3.786
UGs:

| UGs | Com inativo | Vazias | Contratos | `id` repetido |
|---|---|---|---|---|
| 12 | 4 | 8 | 100 | 0 |

O formato é o mesmo do cabeçalho ativo, com `situacao: "Inativo"` e
`justificativa_inativo` preenchida (ex.: `"Rescindido"`). Como dois terços das
UGs não têm inativo, a `contrato_inativo_ingest_dag` não pode reprovar uma
execução sem nenhum contrato da mesma forma que a de ativos faz: com as 10 UGs do
`INGEST_MAX_UGS` local, isso aconteceria por acaso em cerca de 2% das execuções.
A DAG só trata o zero como anomalia da fonte a partir de **50 UGs varridas**,
onde a chance de todas virem vazias por acaso é da ordem de 1e-9.

O mesmo `id` aparece em `raw_contrato_ativo` e em `raw_contrato_inativo` quando o
contrato é inativado entre duas varreduras. A Silver que unifica as duas precisa
deduplicar por `id` **entre** as duas raws, pelo `dt_ingest`.

### Cobertura: UG só com contrato inativo fica de fora

A varredura parte da raw de `unidade_contratante`, isto é, de
`/api/contrato/unidades`. Numa segunda amostra, de 60 UGs dessa lista
(sorteio com semente 7, em **2026-10-07**), as 59 que responderam ao endpoint
de ativos tinham pelo menos um contrato ativo; a 60ª excedeu 180 s nele. A lista
se comporta, então, como lista de UGs **com contrato ativo**, e uma UG cujos
contratos estão todos inativos não é alcançada.

A primeira versão aceita essa lacuna. Fechá-la pede outra fonte de UGs (a issue
#19 propõe `compras_gov.uasg`), o que cria dependência entre sistemas e
multiplica as chamadas por UGs que, na maioria, não têm contrato nenhum. O
tamanho da lacuna ainda não foi medido; isso fica para quando a raw de
`compras_gov.uasg` estiver disponível para comparar as duas listas.

A mesma amostra mostrou menos UGs vazias de inativo do que a de 12: 17 das 60
(28%), contra 8 das 12. Com menos UGs vazias, a chance de uma varredura vir
inteira vazia por acaso só diminui, então o limiar de 50 UGs continua
conservador.

## Escopo de detalhamento dos sub-recursos

Detalhar todos os contratos a cada execução não é viável. O escopo é
parametrizado pela Variable do Airflow **`contratos_gov_escopo_orgaos`**, uma
lista de códigos de órgão:

```json
["46000"]
```

As DAGs de sub-recurso (#20 a #33) leem essa Variable, filtram na raw os
contratos cujas UGs pertencem a esses órgãos e só detalham esses. O default é o
MGI, único órgão consumidor hoje.

No compose, `INGEST_MAX_UGS=10` limita os cabeçalhos (#18 e #19). As dez
primeiras UGs por código não incluem o MGI; portanto, numa raw local recém-criada,
a DAG de empenhos (#22) falha corretamente por não encontrar contratos do órgão
46000. Para validá-la, ingira primeiro cabeçalhos de uma UG do MGI (por exemplo,
201057) nas duas raws ou ajuste **somente no ambiente de teste** a Variable de
escopo para um órgão presente na amostra. Nenhuma dessas escolhas altera o
escopo padrão de homologação ou produção.

**Para ampliar**: acrescente o código do órgão à Variable. O custo cresce com o
número de contratos dos órgãos incluídos, não com o número de órgãos — um órgão
com muitos contratos pesa mais que vários pequenos. Antes de incluir um órgão
grande, vale medir quantos contratos ele tem (`contrato_ativo` já na raw) e
estimar: uma chamada por contrato por sub-recurso.

O resolvedor dessa Variable é `airflow/helpers/contratos_gov_scope.py`, escrito
com a primeira DAG de sub-recurso (#22). Ele valida a lista e une os `id` de
`contrato_ativo` e `contrato_inativo` dos órgãos no escopo; as demais DAGs de
sub-recurso reutilizam as mesmas duas funções.

### Empenhos: o `id` não identifica a linha sozinho

A issue #22 registrou `id` como chave de `contrato_empenho`, com base em
contratos olhados um a um. Medição de **2026-10-08** em 80 contratos ativos da
UG 201057 (órgão 46000), contra `GET /api/contrato/{contrato_id}/empenhos`:

| Contratos | Com empenho | Linhas | `id` distintos | `id` em mais de um contrato | Linhas repetidas no mesmo contrato |
|---|---|---|---|---|---|
| 80 | 24 | 3.205 | 1.627 | 715 | 13 (12 no contrato 93055, 1 no 212249) |

O `id` é o da nota de empenho, e a fonte vincula a mesma nota a vários
contratos: o empenho 13243362 aparece nos contratos 2289, 212249, 213150,
223657, 226366 e 227024. No código da fonte
(`ContratoController::empenhosPorContratoId`, em gitlab.com/comprasnet/contratos)
a consulta parte da tabela de vínculo `contratoempenhos` e devolve
`empenho->id`, sem restrição de unicidade no vínculo.

Daí duas decisões na `contrato_empenho_ingest_dag`:

- a chave primária é `(contrato_id, id)`. Com `id` sozinho, o upsert do backend
  `warehouse` reduziria os 3.192 vínculos distintos da amostra a 1.627 linhas e
  deixaria em cada uma o `contrato_id` do último contrato lido;
- linha idêntica repetida dentro do mesmo contrato é descartada antes da
  escrita, com aviso no log. O contrato 93055 devolveu 563 linhas para 551 ids,
  e as 13 linhas repetidas da amostra eram cópias exatas. Mesmo `id` com
  conteúdo diferente no mesmo contrato continua sendo erro.

Conferido no backend `warehouse` com as mesmas 80 chamadas: 3.192 linhas na
raw, 3.192 pares `(contrato_id, id)` distintos, nenhum par da API faltando, e a
mesma contagem depois de uma segunda execução.

Itens e cronograma foram medidos depois (seções abaixo); os demais sub-recursos
(#20 a #33) ainda não foram medidos dessa forma.

### Itens: `id` sozinho, objeto de data e histórico aninhado

`GET /api/contrato/{contrato_id}/itens` responde sem token, em lista, sem
paginação nem filtro de data. Ao contrário dos empenhos, o payload já traz
`contrato_id` (como número); a `contrato_item_ingest_dag` compara esse valor
com o da URL em toda linha e falha antes da escrita se divergirem.

Medição de **2026-10-12** em 50 contratos do órgão 17000 (escopo local; o
padrão é 46000), com `INGEST_MAX_CONTRATOS=50` no compose:

| Contratos | Com item | Linhas | `id` distintos | `id` em mais de um contrato |
|---|---|---|---|---|
| 50 | 50 | 147 | 147 | 0 |

Daí as decisões da DAG:

- a chave primária é só `id`, como o catálogo declara: na amostra, nenhum `id`
  de item apareceu em mais de um contrato. `id` repetido no mesmo lote
  interrompe a execução em vez de ser descartado, porque a repetição observada
  nos empenhos não apareceu nos itens e descartá-la esconderia o caso;
- `data_inicio_item` é objeto `{date, timezone_type, timezone}` (serialização
  do Carbon/PHP) e `historico_item` é lista de termos. Os dois são gravados
  como JSON no `warehouse` e como objetos nativos no `object_storage`, e a
  Silver converte a data e explode o histórico.

Particularidades do payload que a Silver precisa tratar (contrato 2289, mesma
data):

- `catmatseritem_id` e `tipo_id` chegam como **texto descritivo** (por exemplo,
  `"Serviço"`), não como código. O cruzamento com o catálogo de
  material/serviço do `compras_gov`, previsto no catálogo, não pode usar esse
  campo como está;
- `quantidade`, `valorunitario` e `valortotal` são strings com formatação
  brasileira (`"7.532.888,13"`);
- dentro de `historico_item`, o mesmo `contrato_historico_id` pode aparecer
  mais de uma vez com valores diferentes (235545, duas linhas de 2021-04-16 com
  `valor_unitario` 2,90 e 2,97). Ele não serve sozinho como chave da linha
  explodida.

Conferido no backend `warehouse`: 147 linhas na raw, 147 `id` distintos, igual
ao total que a DAG leu da API, amostra igual à API na mesma data (contrato
166513: 11 itens na raw e na API, com quantidade e valor unitário iguais),
`dt_ingest` e `contrato_id` preenchidos em toda linha, e a mesma contagem
depois de uma segunda execução. O `object_storage` (um Parquet por contrato,
com a data e o histórico preservados) é coberto por
`test_object_storage_preserva_data_e_historico_aninhados`.

### Cronograma: `id` sozinho, payload plano e contrato vazio frequente

`GET /api/contrato/{contrato_id}/cronograma` responde sem token (200), em
lista, sem paginação nem filtro de data. Como no item, o payload já traz
`contrato_id` (como número); a `contrato_cronograma_ingest_dag` compara esse
valor com o da URL em toda linha e falha antes da escrita se divergirem.

Medição de **2026-10-26** em 50 contratos do órgão 17000 (escopo local; o
padrão é 46000), com `INGEST_MAX_CONTRATOS=50` no compose:

| Contratos | Com cronograma | Linhas | `id` distintos | `id` em mais de um contrato | Maior cronograma |
|---|---|---|---|---|---|
| 50 | 40 | 566 | 566 | 0 | 84 parcelas (contrato 167445) |

Daí as decisões da DAG:

- a chave primária é só `id`, como o catálogo declara: na amostra, nenhum `id`
  de parcela apareceu em mais de um contrato. Parcela sem `id` ou `id` repetido
  no mesmo lote interrompe a execução, pelo mesmo motivo do item;
- cronograma vazio é comum (10 dos 50 contratos): conta como contrato vazio,
  não grava e não interrompe o bloco;
- as parcelas de um contrato vão numa escrita só;
- nenhum campo do payload é objeto ou lista, então `write_raw` é chamado sem
  `json_fields`.

Particularidades do payload que a Silver precisa tratar (mesma data):

- `valor` é string com formatação brasileira (`"288.988,93"`);
- `observacao` traz o objeto do termo e chega a 1.429 caracteres na amostra;
  vai para a raw sem truncamento;
- `mesref` e `anoref` chegam separados, como números. `vencimento` caiu no mês
  seguinte ao de referência em todas as 566 linhas (`mesref` 10/2019 →
  `vencimento` 2019-11-01), então o recorte por período precisa escolher qual
  dos dois usar: competência ou vencimento;
- `tipo` diz de qual instrumento vem a parcela (`Contrato`, `Termo Aditivo`,
  `Termo de Apostilamento`, `Empenho`, `Outros`), e `numero` é o número desse
  instrumento, não da parcela;
- `receita_despesa` e `retroativo` vieram constantes na amostra (`Despesa`,
  `Não`) e são texto, não booleano.

Conferido no backend `warehouse`: 566 linhas na raw e 566 `id` distintos, igual
ao total que a DAG leu da API. A amostra bate com a API na mesma data: o
contrato 167433 tem 35 parcelas na raw e na API, os mesmos `id` (51623150 a
51623186) e a mesma soma de `valor` (9.135.988,61). `dt_ingest` e `contrato_id`
estão preenchidos em toda linha, e a contagem se mantém depois de uma segunda
execução (o `dt_ingest` é atualizado pelo upsert). O `object_storage` (um
Parquet por contrato e por run, com a `observacao` inteira) é coberto por
`test_cada_contrato_tem_arquivo_proprio_no_object_storage`.

### Arquivos: `id` sozinho, só metadados, medição contra a API pendente

`GET /api/contrato/{contrato_id}/arquivos` é atendido por
`ContratoController::arquivosPorContratoId`, que lista a tabela
`contrato_arquivos` filtrando por `contrato_id` e por UG sem sigilo, e
serializa cada linha com `Contratoarquivo::arquivoAPI` (gitlab.com/comprasnet/
contratos, branch `master`, lido em **2026-11-25**). Os nove campos são `id`,
`contrato_id`, `tipo`, `processo`, `sequencial_documento`, `descricao`,
`path_arquivo`, `origem` e `link_sei`; nenhum é data, objeto ou lista.

Daí as decisões da `contrato_arquivo_ingest_dag`:

- a chave primária é só `id`: é a chave da própria `contrato_arquivos`, e cada
  linha dessa tabela tem um único `contrato_id`. Ao contrário dos empenhos, não
  há tabela de vínculo que repita o mesmo `id` em vários contratos;
- o payload já traz `contrato_id`; a DAG compara com o da URL em toda linha e
  falha antes da escrita se divergirem, como no item e no cronograma;
- contrato sem anexo (`200 []`) é comum na amostra da issue (0 a 11 por
  contrato): conta como contrato vazio e não interrompe o bloco;
- só a listagem é chamada: `path_arquivo` e `link_sei` seguem como texto, e o
  binário não é baixado (issue #27).

Particularidades do código que a Silver precisa tratar:

- `origem` é calculado a partir de `$this->arquivo`, atributo que o modelo não
  tem (o campo é `arquivos`). A comparação com `'0'` nunca é verdadeira, e o
  campo deve vir sempre `"SEI"`, mesmo para arquivo enviado pelo sistema. Não
  use `origem`; `link_sei` preenchido é o indicador confiável;
- `path_arquivo` é `APP_URL + '/storage/' + arquivos`, e `arquivos` tem cast
  para array no modelo. Se a coluna guardar uma lista, a concatenação produz
  um caminho inválido (ou erro na resposta). Confirmar contra o dado;
- o modelo usa `SoftDeletes`: anexo excluído some da API, mas continua na raw do
  backend `warehouse`, porque o upsert não apaga. A Silver não consegue
  distinguir anexo excluído de anexo antigo só pela raw.

**Pendente**: a medição contra a API (contagem, `id` distintos, amostra e
reexecução no backend `warehouse`). Em 2026-11-25, `contratos.comprasnet.gov.br`
recusou toda conexão a partir da rede de desenvolvimento (`Connection reset by
peer` logo após o TLS, inclusive em `/`). No compose, a seleção já foi
conferida: com o escopo local `["17000"]`, `get_contract_blocks` leu 870
contratos das raws de ativos e inativos e o `INGEST_MAX_CONTRATOS=50` os
reduziu a 50, em 2 blocos — os mesmos contratos das medições de item e
cronograma.

A medição ficou reproduzível, para ser refeita assim que a API aceitar conexão:

- `uv run python -m scripts.contratos_gov.medir_subrecurso arquivos --contratos
  <ids> --distribuicao origem,tipo --preenchido link_sei,path_arquivo` imprime a
  tabela no formato das seções de item e cronograma, a distribuição de `origem`
  (confirma ou não que vem sempre `"SEI"`) e quantas linhas têm `link_sei`. O
  script serve para qualquer sub-recurso por contrato e não imprime valores do
  payload além dos campos pedidos em `--distribuicao`;
- `make test-integration` roda `tests/integration/test_contrato_arquivo_flow.py`:
  reexecução sem duplicar no `warehouse` (upsert por `id`, `dt_ingest`
  atualizado), estrutura de nove campos planos, `id` único entre contratos e raw
  igual à API na mesma data. A parte da API faz skip quando a conexão falha; os
  contratos vêm de `CONTRATOS_GOV_AMOSTRA`.

### Terceirizados: estrutura tirada do código, dado pessoal e DAG desligada

`GET /api/contrato/{contrato_id}/terceirizados` responde sem token, em lista,
sem paginação nem filtro de data, como os outros sub-recursos. A diferença é
que **não há medição**: no levantamento de **2026-09-15** a API devolveu `[]`
nos 6 contratos consultados, e as tentativas de ampliar a amostra em 25 e 26 de
novembro falharam antes de qualquer resposta — `contratos.comprasnet.gov.br`
fecha o handshake TLS, recebe a requisição inteira e reseta a conexão sem
devolver um byte, também em `/` e de duas redes diferentes. Não é DNS, porta
nem TLS: é o outro lado.

Sem dado observado, a estrutura vem do código-fonte público do Comprasnet
Contratos (`Contratoterceirizado::terceirizadoAPI`, gitlab.com/comprasnet/
contratos, `master` em `adb9f9ee`, lido em **2026-11-25**): 19 campos, todos
planos — `id`, `contrato_id`, `usuario`, `funcao_id`,
`descricao_complementar`, `jornada`, `unidade`, `salario`, `custo`,
`escolaridade_id`, `data_inicio`, `data_fim`, `situacao`, `aux_transporte`,
`vale_alimentacao`, `numero_contrato`, `unidade_gestora`,
`unidade_gestora_nome` e `fornecedor_identificador`.

Daí as decisões da `contrato_terceirizado_ingest_dag`:

- a chave primária é só `id`, como o catálogo declara, mas aqui é **hipótese**:
  nenhuma linha foi observada. Vale pelo padrão dos outros sub-recursos, e cai
  se a API mostrar o contrário;
- o payload traz `contrato_id`; a DAG compara com o da URL em toda linha e falha
  antes da escrita se divergirem, como no item, no cronograma e nos arquivos.
  Registro sem `id` ou com `id` repetido no mesmo lote também interrompe;
- contrato sem terceirizado (`200 []`) conta como contrato vazio, não grava e
  não interrompe o bloco — na amostra, foi o caso de todos;
- os terceirizados de um contrato vão numa escrita só, sem `json_fields`,
  porque nenhum campo é objeto ou lista;
- a agenda é sábado 17:00, entre prepostos (16:30) e despesas acessórias
  (17:30), na ordem das fases da issue #15.

Dado pessoal é o que distingue esta entidade. O catálogo a classifica como
`pessoal`, e a DAG trata o log como superfície pública: as mensagens citam
contrato, `id` do registro, posição na lista e contagens, nunca um valor do
payload, porque o log da task é legível por quem não tem acesso à raw
(ADR-0020). `test_log_da_ingestao_nao_expoe_dado_pessoal` guarda a regra em vez
de confiar na convenção.

Particularidades que a Silver precisa tratar, lendo o serializador:

- `usuario` traz **CPF mascarado e nome no mesmo campo** (`***.NNN.NNN-**
  NOME`). Separar os dois é da Silver, e o CPF já chega parcial da origem: não
  serve como chave;
- `fornecedor_identificador` é o CNPJ da empresa, não do trabalhador;
- `funcao_id` e `escolaridade_id` terminam em `_id` mas carregam **rótulo de
  texto** (`"Ajudante"`, `"Ensino médio completo"`), não o id da tabela de
  domínio. Tratar como chave estrangeira daria junção vazia;
- `salario`, `custo`, `aux_transporte` e `vale_alimentacao` chegam como texto
  pt-BR (`"1.200,25"`) e seguem assim para a raw: converter é da Silver;
- o recorte temporal da Silver sai de `data_inicio` e `data_fim`, e `data_fim`
  é nula enquanto a alocação está aberta;
- nulo e inteiro no mesmo lote eram achatados pelo `pandas.json_normalize` do
  `cliente_postgres`, que transformava `jornada` 44 em `44.0` e o nulo no texto
  `'NaN'`. Corrigido nesta branch (`fix(cliente_postgres)`), com
  `test_warehouse_preserva_inteiro_e_nulo` cobrindo o caso.

**A DAG é entregue fora do `dag_selector`**, como a issue #30 pede para o caso
de a entidade continuar vazia. O motivo: ligá-la hoje significaria milhares de
chamadas semanais para uma entidade sem nenhuma linha observada, gravando dado
pessoal cuja estrutura ninguém conferiu contra a fonte. O custo de deixar
desligado é zero, e o de ligar no escuro, não.

A linha do allowlist é uma regex com lookahead
(`data_ingest/contratos_gov/(?!contrato_terceirizado_ingest_dag\.py)`), e não
uma lista de arquivos, para que as próximas DAGs da pasta entrem sem editar o
`dag_selector`. `TestDagSelector`, em
`tests/unit/test_data_ingest_contratos_gov_dags.py`, fixa as três partes: esta
DAG fora, as outras dentro, e uma DAG futura dentro.

Para ligar: refazer a medição com
`uv run python -m scripts.contratos_gov.medir_subrecurso terceirizados --ug
<codigo> --max 200` (a sugestão da issue é UG grande com contrato de serviço
com mão de obra); se vier linha, conferir a chave `id` contra o resultado,
apagar o lookahead e registrar a medição aqui.

### Sobre o código do MGI: 46000, não 48000

Na API do Contratos.gov.br, o órgão **46000** está presente na lista de órgãos
com contrato; **48000 não aparece**. Isso é evidência — não prova — para a
pendência registrada em `catalogo/publicacao/mgi.yml`, onde
`codigo_orgao: "48000"` está com `codigo_orgao_verificado: false` e um comentário
apontando justamente a ambiguidade entre os dois códigos.

Esta nota não altera aquele catálogo, que é de outro pipeline (publicação,
ADR-0019/0020). Fica o dado para quem for fechar a pendência.

## O que continua em aberto

- **Credencial JWT.** Decisão fora desta feature. Se aprovada, os 18 endpoints
  por período passam a valer, a carga vira incremental e a varredura cai de horas
  para minutos — o que tornaria obsoletas a concorrência e a frequência
  decididas aqui.
- **Comportamento sob varredura completa.** Nunca observado. Ver a ressalva da
  decisão de concorrência.
- **Estrutura das entidades da Fase 4.** `contrato_terceirizado`,
  `contrato_despesa_acessoria`, `contrato_ocorrencia` e
  `contrato_domicilio_bancario` vieram vazias em toda a amostra: a chave primária
  declarada no catálogo para elas é hipótese, a confirmar ao implementar a DAG.