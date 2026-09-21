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

**Para ampliar**: acrescente o código do órgão à Variable. O custo cresce com o
número de contratos dos órgãos incluídos, não com o número de órgãos — um órgão
com muitos contratos pesa mais que vários pequenos. Antes de incluir um órgão
grande, vale medir quantos contratos ele tem (`contrato_ativo` já na raw) e
estimar: uma chamada por contrato por sub-recurso.

O resolvedor dessa Variable não foi escrito na fundação: ele entra com a primeira
DAG que precisa dele (#18/#19), para não versionar código que nada chama.

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
