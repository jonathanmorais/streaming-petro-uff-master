# Draft — Capítulo de Avaliação Experimental e Infraestrutura
> Rascunho para dissertação. Baseado nos experimentos realizados com Spark Structured Streaming,
> dataset 3W (Petrobras) e infraestrutura EKS (AWS Kubernetes).

---

## 1. Infraestrutura de Execução Distribuída

### 1.1 Containers: portabilidade e reprodutibilidade

Um desafio recorrente em experimentos computacionais é a reprodutibilidade: o código que
funciona na máquina do pesquisador pode falhar em outro ambiente por diferenças de versão de
bibliotecas, sistema operacional, ou configurações de rede. Containers são uma solução para esse
problema — eles empacotam o código da aplicação junto com todas as suas dependências
(bibliotecas Python, JARs Java, variáveis de ambiente) em uma unidade isolada e portátil chamada
**imagem**. Uma imagem executada como container se comporta de forma idêntica
independentemente de onde rode: no laptop do pesquisador, em um servidor on-premise, ou em
nuvem pública.

Uma analogia útil: um container é como uma **receita culinária selada**. Contém não apenas as
instruções (o código), mas também todos os ingredientes exatos (as dependências), na versão
correta, sem depender do que já existe na cozinha (o servidor). Dois pesquisadores executando a
mesma imagem em servidores diferentes obtêm exatamente o mesmo ambiente de execução.

No benchmark, a imagem `spark-benchmark` foi construída a partir do `Dockerfile.spark-benchmark`
versionado no repositório. Ela contém:

- Apache Spark 3.5.3 (runtime Java 11)
- Python 3 com numpy, pandas, pyarrow e boto3
- JARs do hadoop-aws (3.3.4) e aws-java-sdk-bundle (1.12.262) para acesso ao S3
- O script `run_spark_benchmark.py`

Qualquer pessoa que execute `docker build` a partir desse Dockerfile obtém um ambiente
identicamente configurado. Isso garante que os resultados do benchmark são reproduzíveis e não
dependem de variáveis externas ao repositório.

Em ambientes de produção industrial, a rastreabilidade é um requisito relevante. Containers
permitem registrar exatamente qual versão do software processou qual conjunto de dados: a imagem
é identificada por um hash SHA-256 imutável, que pode ser auditado posteriormente. Isso é
especialmente pertinente para dados de poços offshore, onde os resultados de processamento
podem embasar decisões de segurança operacional.

---

### 1.2 Kubernetes como orquestrador de containers

Se um container é a receita selada, o Kubernetes é o **chefe de cozinha**: decide quantas cópias
de cada container executar, em qual servidor cada uma roda, o que fazer se uma falhar, e como
distribuir os dados entre elas.

O experimento utilizou o **Amazon EKS** (Elastic Kubernetes Service) — a distribuição gerenciada
do Kubernetes na AWS — com um cluster de dois nós do tipo `t3.xlarge` (4 vCPUs, 16 GB de RAM
cada). Sobre esse cluster, foi instalado o **Spark Operator** (v2.5.0), um módulo especializado
que estende a API do Kubernetes com um novo tipo de recurso: `SparkApplication`.

Em vez de executar `spark-submit` manualmente em um terminal, a aplicação é declarada como um
arquivo YAML — configuração como código — e submetida ao cluster com `kubectl apply`. O Spark
Operator lê essa declaração e automaticamente:

1. Cria o pod do driver
2. Aguarda o driver registrar os executors
3. Cria os pods dos executors
4. Monitora o status da execução
5. Encerra e limpa os pods ao término

Isso segue o padrão *Infrastructure as Code*: a configuração do cluster, as credenciais de acesso
ao S3, e os parâmetros do Spark ficam versionados no repositório junto ao código da aplicação.

---

### 1.3 Modelo driver/executor no Kubernetes

No Spark em cluster mode, o trabalho é dividido entre dois papéis distintos, cada um executando
em seu próprio container:

**Driver** — o coordenador. Responsável por construir o plano de execução (DAG), distribuir
partições para os executors, e coletar métricas ao final. No benchmark, o driver também executa
a leitura inicial do dataset 3W do S3 via boto3+pyarrow e a gravação do staging.

**Executors (×4)** — os trabalhadores. Recebem partições do staging gravado no S3, executam as
agregações de janela em paralelo, e escrevem os resultados. Cada executor roda em um pod
separado, potencialmente em nós físicos diferentes.

```
┌─────────────────────────────────────────────────────────┐
│                   AWS EKS (cluster)                     │
│                                                         │
│   ┌──────────────────┐      ┌──────────────────┐       │
│   │      Nó 1        │      │      Nó 2        │       │
│   │  ┌────────────┐  │      │  ┌────────────┐  │       │
│   │  │   Driver   │  │      │  │ Executor 1 │  │       │
│   │  │  container │  │      │  │  container │  │       │
│   │  └────────────┘  │      │  ├────────────┤  │       │
│   │  ┌────────────┐  │      │  │ Executor 2 │  │       │
│   │  │ Executor 3 │  │      │  │  container │  │       │
│   │  │  container │  │      │  └────────────┘  │       │
│   │  └────────────┘  │      │  ┌────────────┐  │       │
│   └──────────────────┘      │  │ Executor 4 │  │       │
│                             │  │  container │  │       │
│                             │  └────────────┘  │       │
│                             └──────────────────┘       │
│                        ↕ lê / escreve                  │
│               S3: s3://streaming-3w/                   │
│               ├── 3w/dataset/   (entrada)              │
│               └── results/spark/ (staging + saída)     │
└─────────────────────────────────────────────────────────┘
  Todos os containers executam a mesma imagem
  spark-benchmark:latest (SHA-256 imutável)
```

Os quatro executors rodam a **mesma imagem** do driver — não há diferença de ambiente entre
eles. O S3 é o único ponto de troca de dados, o que é o padrão arquitetural de pipelines
distribuídos em nuvem (arquitetura *shared-nothing*).

---

### 1.4 Dimensionamento de recursos: paralelismo Spark vs reservas Kubernetes

Uma das descobertas práticas do experimento foi a distinção entre três conceitos de CPU que
coexistem no Spark Operator:

| Campo | Gerenciado por | Significado |
|---|---|---|
| `cores` | Spark | Quantas tasks paralelas o executor executa internamente |
| `coreRequest` | Kubernetes scheduler | Quanto o K8s reserva no nó físico para o pod |
| `coreLimit` | Kubernetes cgroups | Teto de CPU que o pod pode consumir |

O scheduler do Kubernetes não enxerga carga de CPU real — ele agenda pods com base em
**reservas declaradas** (`coreRequest`). Um nó com 10% de uso real pode recusar um novo pod se
as reservas declaradas esgotarem sua capacidade alocável. Isso levou ao erro
`FailedScheduling: Insufficient cpu` nos primeiros testes, mesmo com os nós ociosos.

A solução foi separar o paralelismo interno do Spark da reserva de recursos do Kubernetes:

```yaml
executor:
  cores: 2          # Spark: 2 tasks paralelas por executor
  coreRequest: "700m"  # K8s reserva apenas 0.7 vCPU → cabe nos nós
  coreLimit: "1500m"   # pode usar até 1.5 vCPU quando o nó estiver ocioso
```

É análogo à reserva de assento em avião: o assento está "ocupado" mesmo que o passageiro esteja
dormindo. O avião não redistribui assentos com base em quem está acordado.

---

### 1.5 Credenciais de acesso ao S3 via IRSA

O acesso ao S3 é feito via **IRSA** (IAM Roles for Service Accounts) — sem nenhuma chave de
acesso (`AWS_ACCESS_KEY_ID`) no código ou no YAML. O Kubernetes monta automaticamente um
token de identidade no pod; o SDK da AWS o troca por credenciais temporárias junto ao serviço
STS da AWS. As credenciais expiram automaticamente e são renovadas sem intervenção.

Isso é relevante para dados industriais sensíveis: nenhuma credencial permanente fica exposta
no repositório ou nos logs do cluster.

---

## 2. Dataset: 3W (Petrobras)

O dataset 3W [REF] é uma coleção de séries temporais de sensores de poços de petróleo offshore
da Petrobras, composto por 2.228 arquivos Parquet totalizando aproximadamente 2,8 milhões de
registros. Cada registro representa uma leitura simultânea de dez sensores de um poço:

| Sensor | Descrição |
|---|---|
| P-PDG | Pressão no sensor de fundo de poço |
| P-TPT | Pressão na cabeça do tubing |
| T-TPT | Temperatura na cabeça do tubing |
| P-MON-CKP | Pressão a montante da válvula de controle |
| T-JUS-CKP | Temperatura a jusante da válvula de controle |
| P-JUS-CKP | Pressão a jusante da válvula de controle |
| P-MON-CKGL | Pressão a montante da válvula de gás lift |
| P-JUS-CKGL | Pressão a jusante da válvula de gás lift |
| QGL | Vazão de gás lift |
| QBS | Vazão na saída do separador |

Cada registro é rotulado com uma classe de evento: 0 para operação normal e 1–8 para diferentes
tipos de falha (kick, perda de produção, obstrução de gás lift, entre outros).

### 2.1 Desafios de schema em dados industriais reais

O dataset 3W foi gerado ao longo de anos por diferentes versões do pipeline de coleta da
Petrobras. Sem um schema unificado imposto desde o início, os arquivos apresentam três classes
de inconsistência que evidenciam um desafio comum em dados industriais de longa duração:

**Codec de compressão heterogêneo.** Arquivos mais antigos foram comprimidos com Brotli,
enquanto os mais recentes usam Snappy. O Spark não inclui suporte nativo a Brotli, causando
falha ao encontrar esses arquivos.

**Timestamps em nanossegundos.** O índice temporal foi serializado como
`INT64 (TIMESTAMP(NANOS, false))` — precisão de nanossegundos, comum em sistemas SCADA
industriais. O Spark suporta timestamps nativamente apenas até microssegundos, rejeitando esses
arquivos com `AnalysisException: Illegal Parquet type`.

**Tipo da coluna `class` variável.** A coluna de rótulo foi gravada como `INT32` em arquivos
antigos e `FLOAT64` em arquivos recentes. O leitor vetorizado do Spark não faz coerção implícita
de tipos, causando `SchemaColumnConvertNotSupportedException`.

Essas inconsistências não são erros do dataset — são a realidade de qualquer dado industrial
coletado por sistemas heterogêneos ao longo do tempo. Um pipeline de ingestão robusto precisa
tratá-las. A solução adotada foi substituir o leitor Parquet nativo do Spark por **boto3 +
pyarrow** no driver: o pyarrow suporta todos esses tipos nativamente e normaliza o schema antes
de entregar os dados ao Spark, cujos executors nunca veem um arquivo com tipos incompatíveis.

---

## 3. Avaliação Experimental

### 3.1 Configuração do experimento

| Parâmetro | Valor |
|---|---|
| Engine | Apache Spark Structured Streaming 3.5.3 |
| Modo de execução | Cluster mode (Spark Operator v2.5.0, EKS) |
| Nós do cluster | 2× t3.xlarge (4 vCPU, 16 GB RAM) |
| Driver | 2 cores, 4 GB RAM |
| Executors | 4× (2 cores, 3 GB RAM cada) |
| Dataset | 3W Petrobras — 2.228 arquivos Parquet |
| Registros amostrados | 100.000 (de 2.835.284 disponíveis, ver Seção 3.2) |
| Janela de agregação | 60 segundos |
| Período coberto pelos dados | 2011-08-30 a 2019-08-23 |

### 3.2 Pipeline de processamento

O pipeline é executado em três fases:

**Fase 1 — Staging.** O driver lê os arquivos Parquet do S3 via boto3+pyarrow, normaliza o
schema (TIMESTAMP(NANOS) → int64, class INT32 → float64), e amostra 100.000 registros do
total disponível. Por razões de custo e tempo de execução, o experimento utiliza uma amostra
aleatória controlada pela variável `MAX_ROWS=100000`. A leitura é limitada a 100 arquivos
(de 2.228 disponíveis), selecionados aleatoriamente para garantir cobertura de diferentes poços
e classes de evento. O resultado é gravado em um diretório de staging no S3. Esta fase é
executada inteiramente no driver.

**Fase 2 — Streaming.** O Spark lê o staging com `readStream`, agrupa os registros em janelas
temporais de 60 segundos por classe de evento, e calcula para cada janela: contagem de
registros, média e desvio padrão de cada sensor. O resultado é coletado em memória com
`outputMode("complete")` e `trigger(once=True)`.

**Fase 3 — Métricas.** O driver coleta as métricas de progresso (`lastProgress`), calcula
percentis de latência (P50, P95, P99), e grava o arquivo `metrics.json` no S3 via boto3.

### 3.3 Janelas temporais e o vetor de features

Cada registro do dataset é uma leitura instantânea dos 10 sensores de um poço num momento
específico:

```
timestamp=2015-03-01 10:00:03 | class=0 | P-PDG=182.3 | P-TPT=45.1 | T-TPT=72.4 | ...
timestamp=2015-03-01 10:00:07 | class=0 | P-PDG=183.1 | P-TPT=45.3 | T-TPT=72.1 | ...
timestamp=2015-03-01 10:00:51 | class=0 | P-PDG=181.9 | P-TPT=44.9 | T-TPT=72.6 | ...
```

O operador `groupBy(window("event_time", "60 seconds"), "class")` agrupa todos os registros
de um mesmo intervalo de 60 segundos e classe de evento, calculando média e desvio padrão de
cada sensor:

```
janela [10:00, 10:01) | class=0
  → record_count  = 3
  → avg(P-PDG)   = 182.4    std(P-PDG)   = 0.6
  → avg(P-TPT)   = 45.1     std(P-TPT)   = 0.2
  → avg(T-TPT)   = 72.4     std(T-TPT)   = 0.25
  → ...          (10 sensores × 2 estatísticas = 20 valores)
```

Cada janela comprime N leituras brutas dos 10 sensores em um **vetor de 20 features** —
média e desvio padrão de cada sensor no intervalo — associado a uma classe de evento. Esse
vetor é a unidade de análise do pipeline: em um sistema de detecção de anomalias em produção,
ele seria consumido por um modelo de classificação que compararia o comportamento observado
com padrões históricos de falha.

### 3.4 Resultados

| Métrica | Valor |
|---|---|
| Tempo total de processamento | 31,09 s |
| Throughput | 3.216 registros/s |
| Janelas de 60s processadas | 36.182 |
| Latência end-to-end (P50) | 65,1 s |
| Latência end-to-end (P95) | 65,1 s |
| Latência end-to-end (P99) | 65,1 s |

**Throughput.** O cluster processou 100.000 registros em 31 segundos, resultando em 3.216
registros/segundo distribuídos entre os 4 executors. O gargalo é a fase de staging —
leitura sequencial dos arquivos do S3 no driver — e não a fase de agregação em si.

**Janelas processadas.** Os 100.000 registros, distribuídos por 8 anos de dados, produziram
36.182 janelas de agregação. A maioria das janelas contém poucos registros (mediana = 2,
P95 = 6), o que reflete a natureza dos dados industriais: eventos de falha são raros e os dados
de operação normal são amostrados periodicamente, não em fluxo contínuo.

**Latência end-to-end.** A latência de ~65 segundos é uniforme para todos os percentis
(P50=P95=P99). Isso é uma característica do design do benchmark com `trigger(once=True)`: todos
os registros são ingeridos em um único lote, então todas as janelas são finalizadas
simultaneamente, resultando no mesmo `processing_ts_ms` para todas. A latência medida
representa o tempo total do pipeline desde a ingestão simulada no staging até a saída do
processamento — uma métrica de *end-to-end pipeline latency* para dados em batch.

Em um cenário de streaming real com Kafka, cada evento chegaria com um timestamp próprio e
seria processado no micro-batch seguinte, produzindo uma distribuição de latência com variância
— o que é objeto de trabalhos futuros.

### 3.5 Considerações sobre o modelo micro-batch

O Spark Structured Streaming opera no modelo **micro-batch**: a cada intervalo de trigger, o
engine coleta todos os eventos acumulados desde o último trigger e os processa como um lote.
Com `trigger(once=True)`, há um único micro-batch que processa todos os dados disponíveis no
staging e encerra.

Este modelo tem implicações diretas na latência: eventos que chegam logo após o início de um
micro-batch esperam até o próximo trigger para ser processados. Em contrapartida, o throughput
é alto porque o processamento em lote é mais eficiente que o processamento evento a evento.

Uma limitação identificada durante o experimento: com `trigger(once=True)` e `outputMode
("append")`, o watermark parte de `1970-01-01T00:00:00Z` e só avança no batch seguinte — que
não existe. Resultado: 0 janelas emitidas. A solução foi usar `outputMode("complete")`, que
emite todo o estado de agregação a cada batch independentemente do watermark.

---

## 4. Discussão

Os resultados confirmam a viabilidade do Apache Spark Structured Streaming como plataforma de
processamento de dados de sensores industriais em ambiente de nuvem gerenciada. O pipeline
processou 100.000 registros de séries temporais de poços offshore em 31 segundos, produzindo
36.182 agregações temporais com throughput de 3.216 registros/segundo.

Os principais desafios encontrados não foram de performance, mas de **compatibilidade de dados**:
o dataset 3W, gerado ao longo de uma década por sistemas heterogêneos, apresentou três
inconsistências de schema que exigiram tratamento explícito no pipeline. Isso é representativo
de dados industriais reais e distingue o experimento de benchmarks realizados com datasets
sintéticos ou cuidadosamente normalizados.

A escolha do Kubernetes (EKS) como plataforma de execução, em vez de um ambiente local, foi
motivada pela representatividade: operadores de petróleo que implantariam esse tipo de pipeline
em produção utilizariam infraestrutura equivalente. O uso de containers garante que os
resultados são reproduzíveis e que o ambiente de execução pode ser auditado e replicado
exatamente, o que é um requisito para aplicações em infraestrutura crítica.
