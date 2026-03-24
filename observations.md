# 📊 Benchmark: Flink vs Spark

## 🔢 Eventos processados

| Pipeline | Eventos processados |
|----------|-------------------|
| Flink    | 10.282            |
| Spark    | 10.393            |

**Observação:**  
Quase empatados — Spark com ~111 eventos a mais.  
Ambos estão consumindo continuamente em tempo real.

---

## 📈 Resultados do benchmark

| Métrica              | Spark   | Flink  |
|---------------------|--------|--------|
| Eventos processados | 10.393 | 10.282 |
| Latência média      | 369,7 s| 57,0 s |
| Throughput          | 5,82 ev/s | 5,71 ev/s |

---

## 🧠 Análise

- **Flink ~6,5x mais rápido em latência** (57s vs 370s)
- Spark em modo local sofreu com **batch falling behind**
  - Cada micro-batch levou ~22s em vez de 500ms
  - Isso acumulou uma latência enorme
- **Throughput quase idêntico (~5,7–5,8 ev/s)**
  - Ambos consumiram do mesmo tópico Kafka com 10 wells no mesmo ritmo
- A alta latência do Spark é **esperada em `local[4]`**
  - Em um cluster (ex: Docker/K8s) seria bem menor

---

# ⚙️ Micro-batch no Spark Structured Streaming

O Spark **não processa eventos individualmente** como o Flink.  
Ele opera em **micro-batches**:



- `TRIGGER_INTERVAL = 500 milliseconds`
- A cada 500ms o Spark deveria:
  1. Ler novos eventos do Kafka  
  2. Processar em lote  

---

# ⚠️ Batch Falling Behind

Isso significa:

- Um batch levou **22 segundos**
- O próximo deveria começar em **500ms**
- Resultado: o Spark ficou **atrasado (lagging)**

👉 Enquanto processava um batch, novos eventos continuavam chegando no Kafka.

---

## 🚨 Por que aconteceu aqui?

- Execução em **`local[4]`** (apenas 4 threads na mesma máquina)
- **Janela de agregação de 60 segundos**
- **Shuffle com 200 partições** (alto custo computacional)
- Download inicial dos **JARs do Kafka connector (~2 minutos)**

---

## ⏱️ Impacto na latência

Exemplo:
Evento produzido: 19:00:00
Processado: 19:06:00
Latência: 6 minutos


👉 Isso explica a média de **~370 segundos**

---

## ⚡ Como o Flink se comporta

O Flink usa **streaming real (event-by-event)**:

- Não acumula eventos em batches
- Processa assim que chegam do Kafka

👉 Resultado:

- Latência muito menor (~57s)
- Ainda impactado pela janela de 60s (não pelo engine)

---

## 🧩 Resumo final

| Característica | Spark | Flink |
|--------------|------|------|
| Modelo        | Micro-batch | Streaming real |
| Latência      | Alta (local) | Baixa |
| Throughput    | Similar | Similar |
| Sensível a recursos | Muito | Menos |
| Ideal para    | Batch + streaming híbrido | Streaming puro |

Os resultados estão corretos — o que parece estranho tem uma explicação técnica:

Spark P50=P95=P99=396ms (todos iguais)
Isso é esperado. Com trigger(once=True), o Spark processa todos os dados em um único micro-batch. Todas as janelas são emitidas ao mesmo tempo, então todas têm exatamente o mesmo processing_ts_ms. Resultado: latência uniforme de ~396ms para 100% das janelas. Não há distribuição — é um sistema batch.

Flink P50=6ms, P95=19ms, P99=6760ms (variando)
Flink processa registro por registro. Os primeiros registros são processados rapidamente (6ms), os últimos esperam a fila inteira (6760ms). Por isso há alta variância no tail.

O que o gráfico está mostrando corretamente é:

Spark	Flink
Modelo	Micro-batch (processamento em lote)	True streaming (registro a registro)
Latência mediana	396ms	6ms
Latência P99	396ms (previsível)	6760ms (cauda longa)
Throughput	~54K rec/s	~326 rec/s
Spark: alta throughput, latência uniforme mas alta (~400ms)

Flink: baixa latência mediana, mas cauda longa enorme por ser Python puro sem paralelismo real

Esse é o trade-off clássico entre os dois sistemas. Os resultados fazem sentido para um ambiente local sem cluster.

---

# ☸️ Arquitetura: Spark Operator no EKS

## Como o script roda no cluster

```
kubectl apply -f k8s/spark/spark-benchmark.yaml
          │
          ▼
    Spark Operator (controller)
          │ cria e gerencia
    ┌─────┴────────────────────────────────────────────┐
    │  Driver Pod  (roda run_spark_benchmark.py)        │
    │  - lê dataset do S3 com Spark (não pandas)       │
    │  - grava staging no S3                           │
    │  - coordena os executors                         │
    │  - coleta métricas após awaitTermination()       │
    └──────────────────┬───────────────────────────────┘
                       │ spawna
              ┌────────┴────────┐
              │  Executor Pod 1 │   processa partições do staging
              │  Executor Pod 2 │   processa partições do staging
              └─────────────────┘
                       │ lê/escreve
                       ▼
              S3: s3://bucket/3w/
              ├── dataset/         ← parquets de entrada (read-only)
              └── results/spark/
                  ├── _staging/    ← dados amostrados para o streaming ler
                  ├── stream_results/  ← output das janelas de 60s
                  ├── metrics_raw/ ← JSON com throughput e latências
                  └── _checkpoint/ ← checkpoint do Structured Streaming
```

## Por que não usar `.master("local[*]")`

O `run.py` original usa `SparkSession.builder.master("local[*]")`, que faz o Spark
rodar tudo em um único processo (o driver). Os executor pods criados pelo Operator
ficam ociosos. No `run_spark_benchmark.py`, o `.master()` é omitido — o Operator
injeta automaticamente `spark://driver-svc:7077` via `spark-submit`.

## Fluxo de execução (3 fases)

| Fase | O que acontece | Onde |
|------|---------------|------|
| 1. Staging | Spark lê parquets do S3, amostra MAX_ROWS, grava em `_staging/` | Driver + Executors |
| 2. Streaming | `readStream` lê `_staging/`, agrega janelas de 60s, `trigger(once=True)` | Driver + Executors |
| 3. Métricas | Driver coleta `lastProgress`, calcula p50/p95/p99, salva JSON no S3 | Driver |

## Diferença local vs cluster

| Aspecto | `run.py` (local) | `run_spark_benchmark.py` (EKS) |
|---------|-----------------|-------------------------------|
| Master | `local[*]` (1 processo) | cluster (driver + N executors) |
| Dataset | pandas → tmpdir local | Spark → S3 staging |
| Output | `./results/` local | `s3a://bucket/results/spark/` |
| Credenciais S3 | N/A | IRSA (WebIdentityTokenFile) |
| Escalabilidade | 1 máquina | adicionar `executor.instances` no YAML |

## Credenciais S3 via IRSA (sem secrets hardcoded)

O Spark Operator anota o pod do driver com a IAM Role criada pelo `eksctl create iamserviceaccount`.
O provider `WebIdentityTokenFileCredentialsProvider` lê o token montado automaticamente em
`/var/run/secrets/eks.amazonaws.com/serviceaccount/token` e troca por credenciais temporárias
da AWS STS. Nenhuma `AWS_ACCESS_KEY_ID` é necessária no YAML.

---

# 🔄 Evolução do benchmark no notebook (dataset 3W, Colab Pro)

## Iteração 1 — PyFlink DataStream API (Python puro)

| Métrica | Spark | Flink |
|---|---|---|
| Throughput | ~54K rec/s | ~326 rec/s |
| Latência P50 | ~0ms (negativa por bug) | 6ms |
| Latência P99 | ~0ms | 31.542ms |

**Problema:** PyFlink DataStream processa cada registro cruzando a barreira JVM↔Python individualmente. Resultado: ~165x mais lento que Spark.

---

## Iteração 2 — PyFlink Table API + `from_pandas()`

| Métrica | Spark | Flink |
|---|---|---|
| Throughput | ~44K rec/s | ~7K rec/s |
| Latência P50/P95/P99 | 631ms (uniforme) | 14.229ms (uniforme) |
| Tempo total | 2.3s | 13.9s |

**Melhoria:** Table API compila SQL para bytecode Java — sem loop Python por registro.
**Problema remanescente:** `from_pandas()` copia 100K linhas do Python para a JVM antes de executar. Ainda é o gargalo principal.

---

## Iteração 3 — PyFlink Table API + FileSystem connector (parquet)

Flink lê parquet diretamente do disco via conector nativo da JVM, mesmo mecanismo usado pelo Spark.
Elimina a serialização Python↔JVM dos dados de entrada.

**Por que a latência ainda aparece uniforme (P50=P95=P99)?**
Limitação do design batch: num benchmark sem Kafka, todos os resultados saem ao mesmo tempo. Para latência real por janela seria necessário true streaming com Kafka.

---

## 🧩 Lições aprendidas

| Gargalo | Causa | Fix aplicado |
|---|---|---|
| Flink 165x mais lento | DataStream Python loop | Migrar para Table API SQL |
| Flink 6x mais lento | `from_pandas()` serialização | FileSystem connector + parquet |
| Spark latência negativa | `unix_timestamp()` precisão de 1s | `current_timestamp()` com ms |
| Spark sem latência no gráfico | `trigger(once=True)` append mode | `outputMode("update")` |
| Spark quebrava com parquet | Coluna `timestamp` com NANOS | Drop antes de escrever chunks |
| RAPIDS incompatível | Spark 3.5.8, RAPIDS só até 3.5.2 | RAPIDS removido, Spark roda em CPU |

---

# ☸️ Scheduling de pods no Kubernetes: requests vs uso real

## O problema

Um executor pod ficou preso em `FailedScheduling` com a mensagem:

```
0/2 nodes are available: 2 Insufficient cpu.
```

Mesmo com os nodes usando menos de 10% de CPU real, o scheduler recusou o pod.

## Por que isso acontece

O Kubernetes **não agenda pods com base no uso real de CPU** — ele usa **requests** (reservas declaradas). Um request é uma garantia: o K8s reserva aquela quantidade no node independente de o pod estar usando ou não.

```
Node t3.xlarge: 4 vCPU ≈ 3900m allocatable

  Pod driver:            request = 2000m  → reservado
  Pod executor-1:        request = 2000m  → reservado
  Sistema (kube-proxy,
  coredns, operator...): request ~  450m  → reservado
  ─────────────────────────────────────────────────
  Total reservado:                  4450m → não cabe em 1 node

  Distribuído em 2 nodes:
    Node 1: 2250m reservados → 1750m livres
    Node 2: 2450m reservados → 1550m livres

  Novo executor pedindo 2000m → não cabe em nenhum node
  → FailedScheduling (mesmo com uso real de ~10%)
```

É como reserva de assento em avião: o assento está "ocupado" mesmo que o passageiro esteja dormindo. O avião não redistribui assentos com base em quem está acordado.

## Os três conceitos de CPU no Spark Operator

| Campo | Quem usa | Significado |
|---|---|---|
| `cores` | Spark | Quantas tasks paralelas o executor roda. Spark não sabe nada de K8s. |
| `coreRequest` | Kubernetes scheduler | Quanto o K8s **reserva** no node. Determina onde o pod é colocado. |
| `coreLimit` | Kubernetes cgroups | Máximo que o pod pode usar. Se ultrapassar, é throttled. |

São três camadas ortogonais. `cores: 2` não implica `coreRequest: 2000m` — são configurações independentes.

## Como calcular o coreRequest ideal

O `coreRequest` correto cruza dois lados:

**Lado do node (capacidade disponível):**

```bash
kubectl describe nodes | grep -A 10 "Allocated resources"
```

```
Node t3.xlarge: 4000m total
- 2300m já alocado (sistema + operator + driver)
= 1700m disponível por node
÷ 2 executors no mesmo node = 850m por executor
× 0.80 (margem de segurança) ≈ 680m conservador
```

**Lado do workload (demanda do processo):**

O benchmark 3W faz leitura de parquet do S3, parsing de schema, janelas deslizantes e agregações — um perfil com I/O wait alto e picos de CPU durante agregações:

```
pico de CPU: 1.5–2.0 cores por executor (durante agregações)
média:       0.6–0.8 cores (maior parte é I/O wait esperando S3)
```

**Fluxo recomendado:**

1. Rodar uma vez com `coreRequest` baixo e `coreLimit` alto para não bloquear o scheduler
2. Observar o consumo real com `kubectl top pods -n spark`
3. Usar a **média** observada como `coreRequest` e o **pico** como `coreLimit`

No benchmark 3W, usamos `1200m` como estimativa inicial antes de ter dados reais — valor que cabe nos nodes (resolve o `FailedScheduling`) e não causa starvation de CPU para o workload.

## A solução

Separar o paralelismo Spark do request K8s:

```yaml
executor:
  cores: 2              # Spark usa 2 slots de execução internamente
  coreRequest: "1200m"  # K8s reserva apenas 1.2 vCPU → cabe nos nodes
  coreLimit: "2000m"    # pode usar até 2 vCPU quando o node estiver ocioso
```

Com `coreRequest: 1200m`, o executor cabe no Node 1 (1750m livres). Em horários de baixa utilização, o cgroup permite consumir até `coreLimit` sem precisar re-agendar.

## Quando isso importa

Em clusters compartilhados ou de tamanho fixo (como o benchmark com 2× t3.xlarge), ajustar `coreRequest` para o mínimo necessário é essencial. Em produção com autoscaling, o cluster aumentaria automaticamente o número de nodes — mas para um benchmark de custo controlado, o correto é dimensionar os requests para caber na capacidade existente.

---

# 🕐 Timestamps em nanossegundos no dataset 3W

## O problema

O dataset 3W armazena timestamps no formato `INT64 (TIMESTAMP(NANOS, false))` — precisão de nanossegundos sem timezone. O Spark, por padrão, rejeita esse tipo com:

```
AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))
```

Isso acontece porque o Spark só suporta nativamente timestamps até microssegundos (`TIMESTAMP(MICROS)`). O tipo `NANOS` foi introduzido no Parquet para representar séries temporais de alta frequência — comum em dados industriais de sensores, como os poços offshore do 3W.

## Por que isso importa em datasets industriais

Sistemas SCADA e sensores de poços registram eventos em frequências de 1 Hz a 1 kHz. Em séries temporais de equipamentos críticos, a resolução nanosegundos pode distinguir eventos que aparecem simultâneos em microssegundos. O formato Parquet preserva essa precisão, mas engines de processamento distribuído como Spark foram projetados principalmente para dados de negócio (logs, transações), onde microsegundos são mais que suficientes.

## A solução

```yaml
# spark-benchmark.yaml — sparkConf
"spark.sql.parquet.nanosAsLong": "true"
```

Com essa config, o Spark lê timestamps NANOS como valores `Long` (nanossegundos desde epoch Unix) em vez de tentar converter para `TimestampType`. O dado é preservado sem perda de precisão, e o pipeline decide como interpretar o valor numérico.

## Trade-off

| Abordagem | Precisão | Compatibilidade Spark |
|---|---|---|
| `nanosAsLong=true` | Nanosegundos (total) | Funciona — timestamp vira `Long` |
| `nanosAsLong=false` (padrão) | — | Falha com `IllegalParquetType` |
| Pré-processar para microssegundos | Microssegundos (perde 1000x) | Funciona nativamente |

Para o benchmark 3W, `nanosAsLong=true` é a escolha correta: preserva o dado original e o pipeline converte para timestamp de evento via divisão (`/ 1_000` para microssegundos, ou `/ 1_000_000` para milissegundos conforme necessário).

---

# 🗂️ Inconsistências de schema no dataset 3W

O dataset 3W é composto por centenas de arquivos parquet gerados ao longo de anos por diferentes versões do código de coleta da Petrobras. Cada arquivo foi gravado de forma independente, sem garantia de schema unificado. Isso produz três classes de inconsistência que um engine de processamento distribuído precisa tratar.

## 1. Codec de compressão heterogêneo (Brotli vs Snappy)

Arquivos mais antigos (subdiretórios `1/`, `8/`) foram comprimidos com **Brotli**, enquanto os mais recentes usam **Snappy** ou sem compressão.

**Impacto no Spark:** o Hadoop do Spark não inclui `BrotliCodec` na distribuição padrão. Ao encontrar um arquivo Brotli, o executor falha com:
```
ClassNotFoundException: org.apache.hadoop.io.compress.BrotliCodec
```

**Solução:** re-comprimir o dataset inteiro para Snappy antes de subir para S3. O script `scripts/recompress_3w_s3.ipynb` faz isso usando pyarrow (que suporta Brotli nativamente) no Google Colab.

---

## 2. Tipo físico do índice temporal: TIMESTAMP(NANOS) vs INT64

O índice `DatetimeIndex` do pandas é serializado pelo pyarrow como `INT64` com anotação `TIMESTAMP(NANOS, UTC=false)` em arquivos mais antigos. Arquivos gerados por versões mais recentes do pyarrow podem usar `TIMESTAMP(MICROS)`.

**Impacto no Spark:** o leitor Parquet do Spark rejeita `TIMESTAMP(NANOS)` na fase de análise de schema (driver-side), antes mesmo de executar qualquer task:
```
AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))
```

**Detalhe importante:** configurar `spark.sql.parquet.nanosAsLong=true` via `SparkSession.builder.config()` não funciona em cluster mode porque `.getOrCreate()` devolve a sessão já criada pelo `spark-submit` e ignora os configs do builder. O único modo confiável é `spark.conf.set()` após obter a sessão:

```python
spark = SparkSession.builder.appName("...").getOrCreate()
spark.conf.set("spark.sql.parquet.nanosAsLong", "true")
```

---

## 3. Tipo físico da coluna `class`: INT32 vs DOUBLE

A coluna `class` representa o código de evento do poço (0 = normal, 1–8 = tipos de falha). Em arquivos mais antigos ela foi gravada como `INT32` (inteiro); versões mais recentes do pipeline de coleta a gravam como `FLOAT64` (double).

**Impacto no Spark:** ao usar schema explícito com `DoubleType` para `class`, o leitor vetorizado falha em arquivos com `INT32` físico:
```
SchemaColumnConvertNotSupportedException: column: [class], physicalType: INT32, logicalType: double
```

O leitor vetorizado (`VectorizedParquetRecordReader`) não faz coerção implícita de tipos para evitar overhead de conversão. O leitor row-based, por sua vez, converte automaticamente.

**Solução:**
```python
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
```

Isso ativa o leitor row-based para todos os arquivos Parquet da sessão, com coerção implícita INT32 → double. O custo é uma leve redução de throughput na leitura (o leitor vetorizado é mais rápido em colunas homogêneas), mas é negligenciável para um dataset do tamanho do 3W.

---

## Resumo das inconsistências

| Inconsistência | Arquivos afetados | Erro no Spark | Solução |
|---|---|---|---|
| Brotli codec | `1/`, `8/` (arquivos antigos) | `ClassNotFoundException: BrotliCodec` | Re-comprimir para Snappy via pyarrow |
| TIMESTAMP(NANOS) | Maioria dos arquivos | `AnalysisException: Illegal Parquet type` | `spark.conf.set("spark.sql.parquet.nanosAsLong", "true")` |
| `class` INT32 vs DOUBLE | Subdiretório `8/` | `SchemaColumnConvertNotSupportedException` | `spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")` |

Essas inconsistências são típicas de datasets industriais de longa duração: o schema evolui conforme as ferramentas de coleta são atualizadas, mas os dados históricos não são retroativamente reprocessados. Um pipeline de ingestão robusto precisa lidar com todas as variantes simultaneamente.

---

# 🔧 Solução definitiva para TIMESTAMP(NANOS): boto3 + pyarrow no driver

## O problema raiz

Mesmo com `spark.conf.set("spark.sql.parquet.nanosAsLong", "true")` e `enableVectorizedReader=false`, o erro persistia nos executors:

```
AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))
Task 0 in stage 0.0 failed 4 times
```

O motivo: `spark.conf.set()` altera o `SQLConf` da sessão do **driver**, que é thread-local. Os executors rodam em JVMs separadas e criam seus próprios `SQLConf` ao inicializar — esse não herda o estado do driver. Em cluster mode no EKS, driver e executors são pods diferentes; não há memória compartilhada.

`spark.hadoop.X=Y` no YAML injeta `X=Y` no `HadoopConf` de todos os nós, mas o `ParquetToSparkSchemaConverter` nos executors lê `nanosAsLong` do `SQLConf`, não do `HadoopConf` — portanto não resolve.

## A solução

Substituir `spark.read.parquet()` por **boto3 + pyarrow** para a leitura do dataset original:

```python
# Lê diretamente do S3 sem passar pelo leitor Parquet do Spark
obj = s3.get_object(Bucket=bucket, Key=key)
table = pq.read_table(io.BytesIO(obj["Body"].read()))

# Normaliza em Python puro — sem depender do SQLConf dos executors
for field in table.schema:
    col = table.column(field.name)
    if field.name == "timestamp" and pa.types.is_timestamp(col.type):
        cols[field.name] = col.cast(pa.int64())   # NANOS → int64
    elif field.name == "class":
        cols[field.name] = col.cast(pa.float64())  # INT32 → float64
```

O pyarrow suporta `TIMESTAMP(NANOS)` nativamente sem nenhuma configuração. Após normalizar os tipos, o DataFrame pandas é convertido para Spark com `spark.createDataFrame()` — que só recebe dados com tipos limpos — e gravado em S3 como staging.

## Por que funciona

O Spark só enfrenta os arquivos do dataset original durante a leitura feita pelo driver via boto3+pyarrow. O staging gravado no S3 já contém tipos nativos do Spark (`LongType` para timestamp, `DoubleType` para class, `DoubleType` para sensores). Os executors nunca veem um arquivo com `TIMESTAMP(NANOS)`.

## Trade-off

| Abordagem | Paralelo | NANOS fix | Problema |
|---|---|---|---|
| `spark.read.parquet()` + `nanosAsLong=true` | Sim (executors) | Não funciona em cluster mode | SQLConf não herda para executors |
| boto3+pyarrow no driver | Não (driver single-threaded) | Sim | Leitura sequencial no driver |
| Re-comprimir dataset no Colab (`recompress_3w_s3.ipynb`) | Sim | Sim (permanente) | Custo único de reprocessamento |

Para o dataset 3W com 100K registros amostrados, a leitura sequencial no driver leva ~40s — aceitável. Para um dataset completo (2.8M registros), a solução definitiva de longo prazo é executar o notebook `scripts/recompress_3w_s3.ipynb` que normaliza e re-grava todos os parquets no S3.

---

# 📊 Resultados do benchmark Spark no EKS (cluster mode)

## Configuração do experimento

| Parâmetro | Valor |
|---|---|
| Engine | Spark Structured Streaming 3.5.3 |
| Modo | Cluster mode (Spark Operator, EKS) |
| Nodes | 2× t3.xlarge (4 vCPU, 16 GB cada) |
| Driver | 2 cores, 4 GB |
| Executors | 4× (2 cores, 3 GB cada) |
| Dataset | 3W Petrobras — 2.228 arquivos parquet |
| Registros amostrados | 100.000 (de 2.835.284 disponíveis) |
| Janela de agregação | 60 segundos |
| Output mode | `complete` (memory sink) → batch write no S3 |

## Resultados

| Métrica | Valor |
|---|---|
| Tempo total de processamento | 31,09 s |
| Throughput | 3.216 rec/s |
| Janelas de 60s processadas | 36.182 |
| Latência P50 | 65.070 ms (~65 s) |
| Latência P95 | 65.070 ms (~65 s) |
| Latência P99 | 65.070 ms (~65 s) |
| Latência média | 65.070 ms (~65 s) |
| Intervalo temporal dos dados | 2011-08-30 a 2019-08-23 |

## Interpretação da latência uniforme

A latência de ~65s é **constante e uniforme** (P50=P95=P99). Isso não é um erro — é uma consequência do design do benchmark:

1. Todos os 100K registros recebem o mesmo `producer_ts_ms` no momento da escrita do staging (timestamp de ingestão é único por execução)
2. O streaming processa todos os registros em um único micro-batch (`trigger(once=True)`)
3. `latency_ms = processing_ts_ms − max_producer_ts_ms` ≈ (tempo de staging + tempo de streaming) para todas as janelas

**O que essa latência mede:** tempo total do pipeline desde a ingestão simulada até a saída do processamento — uma métrica válida de *end-to-end pipeline latency* para dados em batch.

**O que não mede:** latência individual de eventos chegando em tempo real (isso exigiria um Kafka producer que emita registros com timestamps reais, um a um, para o streaming consumir).

## Por que `outputMode("complete")` foi necessário

Com `trigger(once=True)` e watermark em modo `append`:

- Batch 0 processa todos os dados, mas o watermark inicia em `1970-01-01T00:00:00Z` (epoch zero)
- Em `append` mode, janelas só são emitidas quando o watermark supera o fim da janela
- O watermark só avança **no batch seguinte** — mas com `trigger(once=True)` não há batch seguinte
- Resultado: 0 janelas emitidas, mesmo com 100.000 registros processados

Confirmado pelos logs: `"watermark": "1970-01-01T00:00:00.000Z"` com `numInputRows: 100000`.

`outputMode("complete")` emite **todo o estado atual** a cada batch, independente do watermark — resolvendo o problema. O parquet sink não suporta `complete` mode, então os resultados são coletados da memory sink e escritos no S3 como batch write.

## Diretório de staging: `_staging` → `staging`

O `FileStreamSource` do Spark ignora recursivamente qualquer arquivo cujo path contenha componentes começando com `_` ou `.` (mesma convenção do HDFS para arquivos de sistema). O staging em `OUTPUT_PATH/_staging` fazia com que **todos os parquets fossem silenciosamente ignorados** pelo streaming, resultando em 0 registros lidos.

Renomear para `OUTPUT_PATH/staging` (sem o `_`) resolveu o problema.