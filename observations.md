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