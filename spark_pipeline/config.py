"""
Configurações do pipeline Apache Spark Structured Streaming para o benchmark 3W.
"""
import os

# ---------------------------------------------------------------------------
# Campos de sensores (mesmos do producer e Flink)
# ---------------------------------------------------------------------------
SENSOR_FIELDS: list[str] = [
    "P-PDG", "P-TPT", "T-TPT", "P-MON-CKP", "T-JUS-CKP",
    "P-JUS-CKP", "P-MON-CKGL", "P-JUS-CKGL", "T-JUS-CKGL",
    "P-MON-SDV-P", "PT-P", "T-MON-CKP", "T-PDG", "QGL", "QBS",
]

# ---------------------------------------------------------------------------
# Tópicos Kafka
# ---------------------------------------------------------------------------
KAFKA_BROKERS: str = os.getenv("KAFKA_BROKERS", "kafka:9092")
SOURCE_TOPIC: str = os.getenv("KAFKA_TOPIC", "3w-sensors")
RESULTS_TOPIC: str = os.getenv("KAFKA_RESULTS_TOPIC", "3w-results")

# ---------------------------------------------------------------------------
# Parâmetros de streaming
# ---------------------------------------------------------------------------
WINDOW_SIZE: str = "60 seconds"
SLIDE_DURATION: str = "1 second"
WATERMARK_DELAY: str = "5 seconds"
TRIGGER_INTERVAL: str = "500 milliseconds"
CHECKPOINT_LOCATION: str = os.getenv(
    "SPARK_CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/3w-pipeline"
)

# ---------------------------------------------------------------------------
# Configuração SparkSession
# ---------------------------------------------------------------------------
SPARK_CONFIG: dict[str, str] = {
    "spark.app.name": "3W-Benchmark-SparkSS",
    "spark.master": os.getenv("SPARK_MASTER", "spark://spark-master:7077"),

    # Streaming checkpoint
    "spark.sql.streaming.checkpointLocation": CHECKPOINT_LOCATION,

    # State store backend RocksDB (comparação justa com Flink)
    "spark.sql.streaming.stateStore.providerClass": (
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
    ),

    # Kafka connector (Spark 3.5 + Scala 2.12)
    "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",

    # Memória
    "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
    "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "1g"),

    # Monitoramento Prometheus
    "spark.ui.prometheus.enabled": "true",
    "spark.metrics.conf.*.sink.prometheus.class": (
        "org.apache.spark.metrics.sink.PrometheusServlet"
    ),
}

# Push gateway para métricas Python
PUSHGATEWAY_URL: str = os.getenv("PUSHGATEWAY_URL", "http://localhost:9091")
