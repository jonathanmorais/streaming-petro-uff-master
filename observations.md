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

## A solução

Separar o paralelismo Spark do request K8s:

```yaml
executor:
  cores: 2          # Spark usa 2 slots de execução internamente
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