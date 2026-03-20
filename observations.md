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