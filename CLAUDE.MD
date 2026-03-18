# CLAUDE.md — Benchmark: Ecossistema Apache para Digital Twins em O&G
## Avaliação de Kafka + Flink vs Kafka + Spark SS com o Dataset 3W da Petrobras

---

## 1. VISÃO GERAL DO PROJETO

Este projeto implementa um **benchmark de infraestrutura de stream processing** comparando dois pipelines:

- **Pipeline A**: Apache Kafka → Apache Flink
- **Pipeline B**: Apache Kafka → Apache Spark Structured Streaming

O **dataset 3W da Petrobras** (https://github.com/petrobras/3W) é usado como **workload industrial** — séries temporais reais de sensores de poços offshore. O objetivo NÃO é detectar anomalias: é medir o desempenho das ferramentas (latência, throughput, escalabilidade, fault tolerance) sob carga de dados reais de O&G.

### Dimensões do benchmark
| Dimensão | Métricas |
|---|---|
| Latência | p50, p95, p99 fim-a-fim (ms): produtor Kafka → saída Flink/Spark |
| Throughput | Máximo sustentável (eventos/s) antes de consumer lag acumular |
| Escalabilidade | Comportamento ao aumentar workers (1→5) e partições Kafka (4→32) |
| Fault Tolerance | Tempo de recuperação após falha de worker; integridade exactly-once |

---

## 2. ESTRUTURA DO DATASET 3W

### Clonagem e organização
```bash
git clone https://github.com/petrobras/3W.git
cd 3W
pip install -r requirements.txt
```

### Estrutura de diretórios
```
3W/
  dataset/
    dataset.ini          # metadados e descrições das variáveis
    0/                   # Normal Operation
    1/                   # Abrupt Increase of BSW
    2/                   # Spurious Closure of DHSV
    3/                   # Severe Slugging
    4/                   # Flow Instability
    5/                   # Rapid Productivity Loss
    6/                   # Quick Restriction in PCK
    7/                   # Scaling in PCK
    8/                   # Hydrate in Production Line
  toolkit/
    __init__.py
    base.py              # constantes: VARS, EVENT_NAMES, COLUMNS_DATA_FILES, etc.
```

### Formato dos arquivos Parquet
- **Engine**: `pyarrow`, compressão `brotli`
- **Index**: `timestamp` (datetime, 1 observação por segundo)
- **Frequência de aquisição**: 1 Hz (1 observação/segundo)
- **Tipos**: variáveis = `float`, labels = `Int64` (nullable)

### Nomenclatura dos arquivos
- Instâncias reais: `REAL_<id_do_primeiro_timestamp>.parquet`
- Instâncias simuladas: `SIMULATED_<n>.parquet`
- Instâncias desenhadas: `DRAWN_<n>.parquet`

### Variáveis (27 variáveis no v2.0.0)
```python
# Pressões [Pa]
"P-PDG"        # Downhole pressure at PDG (Permanent Downhole Gauge)
"P-TPT"        # Pressure at TPT (Temperature & Pressure Transducer)
"P-MON-CKP"    # Upstream pressure of PCK (production choke) — MON = montante = upstream
"P-JUS-CKP"    # Downstream pressure of PCK — JUS = jusante = downstream
"P-MON-CKGL"   # Upstream pressure of GLCK (gas lift choke)
"P-JUS-CKGL"   # Downstream pressure of GLCK
"P-MON-SDV-P"  # Upstream pressure of production SDV (shutdown valve)
"PT-P"         # Xmas-tree pressure downstream of PWV (production wing valve)
"P-TPT"        # Xmas-tree pressure at TPT

# Temperaturas [°C]
"T-TPT"        # Temperature at TPT
"T-JUS-CKP"    # Downstream temperature of PCK
"T-MON-CKP"    # Upstream temperature of PCK
"T-PDG"        # Temperature at PDG
"T-JUS-CKGL"   # Downstream temperature of GLCK (discontinued in v2 → NaN in real instances)

# Vazões [m3/s]
"QGL"          # Gas lift flow rate
"QBS"          # Flow rate at SP (service pump)

# Labels
"class"        # Int64: 0=Normal, 1-8=Steady-state anomaly, 101-108=Transient
"state"        # Int64: operational state label
```

### Classes de eventos
```python
EVENT_CODES = {
    0:   "Normal Operation",
    1:   "Abrupt Increase of BSW",
    2:   "Spurious Closure of DHSV",
    3:   "Severe Slugging",
    4:   "Flow Instability",
    5:   "Rapid Productivity Loss",
    6:   "Quick Restriction in PCK",
    7:   "Scaling in PCK",
    8:   "Hydrate in Production Line",
    # Transient states: 101-108 (onset of events 1-8)
}
```

### Leitura com a toolkit
```python
import pandas as pd

def load_instance(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path, engine="pyarrow")
    # Index = timestamp (DatetimeIndex)
    # Columns = variáveis (float) + "class" (Int64) + "state" (Int64)
    return df

# Iterar todos os arquivos de um evento
import pathlib

def iter_event_instances(dataset_path: str, event_code: int):
    event_dir = pathlib.Path(dataset_path) / str(event_code)
    for parquet_file in sorted(event_dir.glob("*.parquet")):
        well_id = parquet_file.stem          # ex: REAL_20180905204436
        source  = well_id.split("_")[0]      # REAL | SIMULATED | DRAWN
        df = pd.read_parquet(parquet_file, engine="pyarrow")
        yield well_id, source, df
```

---

## 3. ESTRUTURA DO PROJETO

```
benchmark-3w/
├── CLAUDE.md                    # este arquivo
├── docker-compose.yml           # Kafka + Zookeeper + Flink + Spark + Prometheus + Grafana
├── .env                         # variáveis de ambiente (DATASET_PATH, KAFKA_BROKERS, etc.)
├── requirements.txt
│
├── producer/                    # Kafka Producer (Python)
│   ├── producer.py              # produtor principal
│   ├── replay.py                # lógica de replay temporal do 3W
│   ├── schema.py                # schema JSON das mensagens
│   └── config.py
│
├── flink_pipeline/              # Pipeline A: Flink
│   ├── pipeline.py              # PyFlink job
│   ├── operators.py             # operadores stateful customizados
│   └── config.py
│
├── spark_pipeline/              # Pipeline B: Spark SS
│   ├── pipeline.py              # Spark Structured Streaming job
│   ├── transformations.py
│   └── config.py
│
├── benchmark/
│   ├── scenarios.py             # definição dos cenários (1x, 5x, 10x taxa)
│   ├── metrics_collector.py     # coleta de métricas Prometheus
│   ├── fault_injector.py        # injeção de falhas (kill worker)
│   └── report.py               # geração de relatórios CSV/JSON
│
├── airflow_dags/
│   └── benchmark_dag.py         # DAG Airflow para orquestrar experimentos
│
└── results/
    └── raw/                     # resultados brutos dos experimentos
```

---

## 4. DOCKER COMPOSE — INFRAESTRUTURA COMPLETA

O `docker-compose.yml` deve subir:

```yaml
# Serviços necessários:
# - zookeeper (kafka dependency)
# - kafka (broker, 3 nodes para fault tolerance tests)
# - kafka-ui (opcional, para debug)
# - flink-jobmanager
# - flink-taskmanager (escalonável: 1→5 replicas)
# - spark-master
# - spark-worker (escalonável: 1→5 replicas)
# - prometheus
# - grafana
# - pushgateway (para métricas do producer Python)

# Redes:
# - benchmark-net (todos os serviços)

# Volumes:
# - dataset volume: monta o diretório local do 3W
# - results volume: persiste resultados dos experimentos
# - prometheus config
# - grafana dashboards
```

Configurações importantes:
- Kafka: `num.partitions=8`, `replication.factor=3`, `retention.ms=604800000` (7 dias)
- Flink: `taskmanager.numberOfTaskSlots=4`, `state.backend=rocksdb`, `execution.checkpointing.interval=10s`
- Spark: `spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints`, trigger `processingTime=500ms`

---

## 5. KAFKA PRODUCER — IMPLEMENTAÇÃO DETALHADA

### Objetivo
Reproduzir os arquivos Parquet do 3W como stream contínuo no Kafka, preservando os timestamps originais e permitindo controle da velocidade de replay (speed_factor).

### Schema das mensagens Kafka

```python
# Tópico: "3w-sensors"
# Chave: well_id (string) → garante ordenação por poço (partição dedicada)
# Valor: JSON

MESSAGE_SCHEMA = {
    "well_id":     str,    # ex: "REAL_20180905204436"
    "source":      str,    # "REAL" | "SIMULATED" | "DRAWN"
    "event_code":  int,    # 0-8 (diretório pai)
    "timestamp":   str,    # ISO 8601: "2018-09-05T20:44:36Z"
    "timestamp_ms": int,   # Unix timestamp em milissegundos (para latência)
    "producer_ts_ms": int, # timestamp de publicação no Kafka (para latência fim-a-fim)
    "P-PDG":      float,   # NaN → None (JSON null)
    "P-TPT":      float,
    "T-TPT":      float,
    "P-MON-CKP":  float,
    "T-JUS-CKP":  float,
    "P-JUS-CKGL": float,
    "T-JUS-CKGL": float,
    "QGL":        float,
    "PT-P":       float,
    "P-MON-CKGL": float,
    "P-MON-SDV-P": float,
    "T-MON-CKP":  float,
    "T-PDG":      float,
    "QBS":        float,
    "class":      int,     # label 0-8 ou 101-108
    "state":      int,
    "instance_progress": float,  # 0.0-1.0, percentual dentro da instância
}
```

### Lógica de replay temporal

```python
# producer/replay.py
# 
# O producer lê cada arquivo Parquet linha por linha.
# Para cada linha, calcula o delta de tempo em relação à observação anterior
# e dorme por (delta / speed_factor) segundos antes de publicar.
#
# speed_factor=1.0  → tempo real (1 evento/segundo por poço)
# speed_factor=5.0  → 5x mais rápido (carga 5x)
# speed_factor=10.0 → 10x mais rápido (carga 10x)
#
# Para simular múltiplos poços simultâneos, cada well_id roda em uma thread separada.
# O número de threads = número de instâncias carregadas simultaneamente.
#
# Tratamento de NaN: float('nan') → None no JSON para compatibilidade com Kafka
```

### Parâmetros do producer

```python
# producer/config.py
PRODUCER_CONFIG = {
    "bootstrap_servers": "localhost:9092",   # ou env KAFKA_BROKERS
    "topic": "3w-sensors",
    "acks": "1",                             # balanço latência/durabilidade
    "compression_type": "lz4",              # compressão eficiente para JSON numérico
    "batch_size": 16384,                    # 16KB
    "linger_ms": 5,                         # aguarda 5ms para batching
    "max_in_flight_requests_per_connection": 5,
}

REPLAY_CONFIG = {
    "dataset_path": "/data/3w/dataset",     # via env DATASET_PATH
    "speed_factor": 1.0,                    # ajustável: 1x, 5x, 10x
    "event_codes": [0, 1, 2, 3, 4, 5, 6, 7, 8],  # todos os eventos
    "sources": ["REAL", "SIMULATED"],       # instâncias a incluir
    "max_concurrent_wells": 42,             # máximo de poços simultâneos
    "loop": True,                           # reinicia do início ao terminar
    "add_producer_timestamp": True,         # essencial para medir latência
}
```

### Criação do tópico Kafka

```bash
# Criar tópico com 8 partições (para testes de escalabilidade até 32)
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic 3w-sensors \
  --partitions 8 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config max.message.bytes=1048576

# Verificar
kafka-topics.sh --describe --topic 3w-sensors --bootstrap-server localhost:9092
```

---

## 6. PIPELINE A — APACHE FLINK

### Estrutura do job PyFlink

```python
# flink_pipeline/pipeline.py
#
# O job Flink consome o tópico 3w-sensors e executa:
#
# 1. SOURCE: KafkaSource com event-time processing
#    - Watermark strategy: BoundedOutOfOrderness(5 seconds)
#    - Timestamp extractor: campo "timestamp_ms" da mensagem
#
# 2. DESERIALIZE: JSON → Row (schema definido em schema.py)
#
# 3. KEY BY: well_id
#    - Garante que observações do mesmo poço vão para o mesmo operador
#    - Permite estado por poço (histórico de janela, contador de eventos)
#
# 4. PROCESS: WellStreamOperator (stateful)
#    - Mantém estado por well_id: last_N_observations (janela deslizante de 60s)
#    - Calcula features simples: mean, std, diff das variáveis principais
#    - Emite evento de saída com latência calculada
#
# 5. SINK: KafkaSink para tópico "3w-results"
#    - Inclui: well_id, event_code, processing_latency_ms, consumer_ts_ms
#
# MÉTRICAS COLETADAS (via Flink metrics API → Prometheus):
# - records_in_per_second (por operador)
# - records_out_per_second
# - latency (p50, p95, p99)
# - currentOffsetLag (consumer lag Kafka)
# - numRestarts (fault recovery)
# - checkpointDuration
# - checkpointSize
```

### Configurações essenciais do Flink

```python
# flink_pipeline/config.py
FLINK_CONFIG = {
    # State backend
    "state.backend": "rocksdb",
    "state.backend.incremental": "true",          # checkpoints incrementais
    "state.checkpoints.dir": "file:///tmp/flink-checkpoints",
    
    # Checkpointing
    "execution.checkpointing.interval": "10s",
    "execution.checkpointing.mode": "EXACTLY_ONCE",
    "execution.checkpointing.timeout": "60s",
    "execution.checkpointing.max-concurrent-checkpoints": "1",
    
    # Paralelismo
    "parallelism.default": "4",                   # ajustar por experimento
    
    # Kafka consumer
    "kafka.bootstrap.servers": "kafka:9092",
    "kafka.group.id": "flink-3w-consumer",
    "kafka.auto.offset.reset": "earliest",
    
    # Network
    "taskmanager.network.memory.fraction": "0.1",
    "taskmanager.memory.process.size": "2048m",
}
```

### Operador stateful por poço

```python
# flink_pipeline/operators.py
#
# class WellStreamOperator(KeyedProcessFunction):
#     - state: ValueState<List[dict]> → últimas 60 observações (janela 60s)
#     - state: ValueState<int> → contador de eventos anômalos
#
#     def process_element(self, value, ctx):
#         # 1. Atualizar janela deslizante
#         # 2. Calcular latência: ctx.timestamp() - value["producer_ts_ms"]
#         # 3. Emitir resultado com métricas de latência
#         # 4. Registrar timer para limpeza de estado (30 min sem dados)
#
# A lógica do operador deve ser MINIMAL para não contaminar
# a medição de desempenho da infraestrutura com custo de ML.
# O objetivo é simular uma operação stateful realista (equivalente
# ao que um DT faria), não implementar detecção de anomalias de fato.
```

---

## 7. PIPELINE B — APACHE SPARK STRUCTURED STREAMING

### Estrutura do job Spark SS

```python
# spark_pipeline/pipeline.py
#
# O job Spark SS é FUNCIONALMENTE EQUIVALENTE ao Flink para fins de comparação:
#
# 1. SOURCE: readStream com KafkaSource
#    - startingOffsets: "earliest"
#    - kafka.bootstrap.servers
#
# 2. PARSE: from_json() com schema idêntico ao Flink
#
# 3. WATERMARK + WINDOW:
#    - withWatermark("event_timestamp", "5 seconds")
#    - groupBy(window("event_timestamp", "60 seconds", "1 second"), "well_id")
#    - Equivalente funcional ao KeyedProcessFunction do Flink
#
# 4. AGGREGATIONS:
#    - Mesmas features calculadas no Flink (mean, std de P-PDG, T-JUS-CKP, etc.)
#    - Inclui cálculo de latência: current_timestamp() - from_unixtime(producer_ts_ms/1000)
#
# 5. SINK: writeStream para Kafka (tópico "3w-results")
#    - trigger: processingTime="500ms" (modo padrão micro-batch)
#    - outputMode: "update"
#    - checkpointLocation: /tmp/spark-checkpoints/3w-pipeline
#
# MÉTRICAS COLETADAS (via Spark UI / StreamingQueryListener → Prometheus):
# - inputRowsPerSecond
# - processedRowsPerSecond  
# - triggerExecution.latency
# - batchDuration
# - numInputRows
# - offsetsBehindLatest (consumer lag)
```

### Configurações essenciais do Spark SS

```python
# spark_pipeline/config.py
SPARK_CONFIG = {
    "spark.app.name": "3W-Benchmark-SparkSS",
    "spark.master": "spark://spark-master:7077",
    
    # Streaming
    "spark.sql.streaming.checkpointLocation": "/tmp/spark-checkpoints",
    
    # State store backend (RocksDB para comparação justa com Flink)
    "spark.sql.streaming.stateStore.providerClass":
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    
    # Kafka
    "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    
    # Memória
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g",
    
    # Monitoramento
    "spark.metrics.conf.*.sink.prometheus.class":
        "org.apache.spark.metrics.sink.PrometheusServlet",
    "spark.ui.prometheus.enabled": "true",
}
```

---

## 8. COLETA DE MÉTRICAS

### Métricas de latência fim-a-fim

```python
# benchmark/metrics_collector.py
#
# A latência é calculada inserindo dois timestamps na mensagem:
# 1. "timestamp_ms": timestamp original do sensor (event-time)
# 2. "producer_ts_ms": momento em que o producer publicou no Kafka (processing-time)
#
# O consumer (Flink/Spark) adiciona:
# 3. "consumer_ts_ms": momento em que a mensagem foi processada pelo operador
#
# Latência fim-a-fim = consumer_ts_ms - producer_ts_ms
# Event-time lag     = producer_ts_ms - timestamp_ms (atraso de replay)
#
# Todas as métricas são exportadas via Prometheus push gateway
# com labels: engine=flink|spark, scenario=1x|5x|10x, partitions=4|8|16|32
```

### Métricas Prometheus coletadas

```yaml
# Métricas expostas pelo producer (via push gateway):
3w_producer_messages_total{well_id, event_code, source}
3w_producer_messages_per_second{scenario}
3w_producer_kafka_lag_bytes

# Métricas Flink (via Flink metrics → Prometheus):
flink_taskmanager_job_task_numRecordsInPerSecond
flink_taskmanager_job_task_numRecordsOutPerSecond  
flink_jobmanager_job_numRestarts
flink_taskmanager_job_latency_source_id_operator_id_taskmanager_id_quantile{quantile="0.5|0.95|0.99"}
flink_jobmanager_job_lastCheckpointDuration
flink_jobmanager_job_lastCheckpointSize

# Métricas Spark (via Spark UI / PrometheusServlet):
spark_streaming_inputRowsPerSecond
spark_streaming_processedRowsPerSecond
spark_streaming_batchDuration
spark_streaming_triggerExecutionLatency

# Consumer lag Kafka (via kafka-consumer-groups.sh ou Prometheus Kafka exporter):
kafka_consumergroup_lag{group, topic, partition}
```

---

## 9. CENÁRIOS DO BENCHMARK

### Matriz de experimentos

```python
# benchmark/scenarios.py
SCENARIOS = [
    # Cenário baseline (velocidade real do dataset)
    {"name": "baseline_1x",    "speed_factor": 1.0,  "workers": 2, "partitions": 4},
    
    # Escalabilidade de carga (stress test)
    {"name": "load_5x",        "speed_factor": 5.0,  "workers": 2, "partitions": 4},
    {"name": "load_10x",       "speed_factor": 10.0, "workers": 2, "partitions": 4},
    
    # Escalabilidade horizontal de workers (mantendo carga 5x)
    {"name": "scale_1w",       "speed_factor": 5.0,  "workers": 1, "partitions": 4},
    {"name": "scale_2w",       "speed_factor": 5.0,  "workers": 2, "partitions": 4},
    {"name": "scale_3w",       "speed_factor": 5.0,  "workers": 3, "partitions": 8},
    {"name": "scale_5w",       "speed_factor": 5.0,  "workers": 5, "partitions": 8},
    
    # Escalabilidade de partições Kafka (mantendo workers=3)
    {"name": "partitions_4",   "speed_factor": 5.0,  "workers": 3, "partitions": 4},
    {"name": "partitions_8",   "speed_factor": 5.0,  "workers": 3, "partitions": 8},
    {"name": "partitions_16",  "speed_factor": 5.0,  "workers": 3, "partitions": 16},
    {"name": "partitions_32",  "speed_factor": 5.0,  "workers": 3, "partitions": 32},
    
    # Fault tolerance (injeção de falha no worker 2 após 2 min estável)
    {"name": "fault_worker",   "speed_factor": 5.0,  "workers": 3, "partitions": 8,
     "fault": {"type": "kill_worker", "target": 2, "after_seconds": 120}},
]

# Duração de cada experimento: 10 minutos (3 min warm-up + 7 min coleta)
EXPERIMENT_DURATION_S = 600
WARMUP_DURATION_S = 180
```

---

## 10. INJEÇÃO DE FALHAS

```python
# benchmark/fault_injector.py
#
# Para o cenário de fault tolerance:
#
# 1. Aguarda estabilização (warm-up + 2 min estável)
# 2. Registra timestamp T_fault (momento da falha)
# 3. Mata container do worker via docker kill / kubectl delete pod
# 4. Monitora consumer lag e output rate até retorno ao estado normal
# 5. Registra timestamp T_recovery (quando consumer lag volta a zero)
# 6. Calcula: recovery_time = T_recovery - T_fault
# 7. Verifica integridade: conta mensagens duplicadas (violação exactly-once)
#
# Métricas coletadas no cenário de fault:
# - recovery_time_seconds
# - messages_lost (esperado: 0 com exactly-once)
# - messages_duplicated (esperado: 0 com exactly-once)
# - throughput_during_recovery (degradação durante recuperação)
```

---

## 11. DAG AIRFLOW — ORQUESTRAÇÃO

```python
# airflow_dags/benchmark_dag.py
#
# DAG: benchmark_3w_pipeline
# Schedule: manual (não periódico)
#
# Fluxo para CADA CENÁRIO × CADA ENGINE:
#
# [setup_topic] → [start_engine] → [start_producer] → [warmup_wait] →
# [start_metrics_collection] → [run_experiment] → [stop_metrics] →
# [stop_producer] → [stop_engine] → [collect_results] → [generate_report]
#
# Paralelismo: Flink e Spark SS NÃO rodam simultâneamente
# (para não contaminar as métricas com contenção de recursos)
#
# Tasks:
# - setup_topic: (re)cria tópico com partitions configurado no cenário
# - start_engine: sobe flink-job ou spark-job via docker-compose scale
# - start_producer: inicia producer com speed_factor do cenário
# - warmup_wait: aguarda 180s (warm-up period)
# - start_metrics_collection: inicia coleta no Prometheus
# - run_experiment: aguarda duração do experimento (420s)
# - stop_metrics: coleta snapshot final do Prometheus
# - collect_results: salva CSVs em results/raw/<scenario>/<engine>/
# - generate_report: gera JSON consolidado com percentis de latência
```

---

## 12. SCRIPTS DE EXECUÇÃO RÁPIDA

```bash
# 1. Subir toda a infraestrutura
docker-compose up -d

# 2. Aguardar Kafka estar pronto
docker-compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# 3. Criar tópico
python benchmark/scenarios.py --setup-topic --partitions 8

# 4. Rodar um único experimento manualmente (debug)
python producer/producer.py --speed-factor 1.0 --max-wells 5 --duration 120 &
python flink_pipeline/pipeline.py --parallelism 2 &

# 5. Rodar benchmark completo via Airflow
airflow dags trigger benchmark_3w_pipeline

# 6. Ver resultados
python benchmark/report.py --results-dir results/raw/ --output results/summary.json
```

---

## 13. CONVENÇÕES DE CÓDIGO

### Python
- Python 3.10+
- Type hints obrigatórios
- Docstrings em português (alinha com o contexto da dissertação)
- Logging via `structlog` (structured logging compatível com Prometheus)
- Tratamento explícito de `NaN` nas variáveis do 3W (comum em dados reais)

### Nomenclatura
- `well_id`: identificador do poço (REAL_XXXX | SIMULATED_N | DRAWN_N)
- `event_code`: código numérico 0-8 (diretório do dataset)
- `speed_factor`: multiplicador de velocidade do replay (1x = tempo real)
- `producer_ts_ms`: timestamp de publicação Kafka em milissegundos
- `consumer_ts_ms`: timestamp de recebimento/processamento pelo engine
- `e2e_latency_ms`: latência fim-a-fim = consumer_ts_ms - producer_ts_ms

### Variáveis de ambiente (.env)
```bash
DATASET_PATH=/data/3w/dataset          # caminho local do dataset clonado
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=3w-sensors
KAFKA_RESULTS_TOPIC=3w-results
FLINK_JM_HOST=localhost
FLINK_JM_PORT=8081
SPARK_MASTER=spark://localhost:7077
PROMETHEUS_URL=http://localhost:9090
RESULTS_PATH=./results
```

---

## 14. PONTOS DE ATENÇÃO

### Sobre o dataset 3W
- **NaN é esperado e frequente**: ~30-40% dos valores podem ser NaN em instâncias reais. Não tratar como erro.
- **Frequência 1 Hz**: cada arquivo tem 1 linha por segundo. Uma instância típica tem 1.000–50.000 linhas.
- **Timestamps são reais**: preservar os timestamps originais é crítico para o replay temporal correto.
- **well_id como chave Kafka**: garante que todas as observações de um poço vão para a mesma partição, preservando ordenação temporal.
- **class vs state**: `class` é o label do evento (0-8, 101-108); `state` é o estado operacional da válvula. Para o benchmark, `class` é suficiente.

### Sobre a comparação Flink vs Spark SS
- Implementar **funcionalidade equivalente** nos dois pipelines é essencial para comparação justa.
- O Spark SS no modo micro-batch tem latência mínima determinada pelo trigger interval — configure em `500ms` para ser competitivo com Flink.
- O Flink deve usar **RocksDB** como state backend (não FsStateBackend) para comparação justa com o RocksDB do Spark SS.
- **Exactly-once** deve estar habilitado em ambos — é o cenário relevante para DTs em O&G.

### Sobre as métricas
- A **latência fim-a-fim** é a métrica mais importante para DTs — mede o tempo entre o sensor e o output do pipeline.
- O **consumer lag** do Kafka é o indicador de throughput máximo sustentável — quando cresce, o throughput foi excedido.
- Coletar **percentis** (p50, p95, p99), não apenas médias — a cauda da distribuição é crítica para alertas operacionais.

---

## 15. REFERÊNCIAS DO PROJETO

- Repositório 3W: https://github.com/petrobras/3W
- Paper 3W Dataset 2.0.0: https://arxiv.org/abs/2507.01048
- Vargas et al. (2019): DOI 10.1016/j.petrol.2019.106223
- Apache Flink Docs: https://nightlies.apache.org/flink/flink-docs-stable/
- Apache Spark SS Docs: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Apache Kafka Docs: https://kafka.apache.org/documentation/
- Prometheus + Flink: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/
- Henning & Hasselbring (2024): DOI 10.1016/j.jss.2023.111879 (referência de metodologia de benchmark)