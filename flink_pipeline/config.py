"""
Configurações do pipeline Apache Flink para o benchmark 3W.
"""
import os

# ---------------------------------------------------------------------------
# Campos de sensores (mesmos do producer)
# ---------------------------------------------------------------------------
SENSOR_FIELDS: list[str] = [
    "P-PDG", "P-TPT", "T-TPT", "P-MON-CKP", "T-JUS-CKP",
    "P-JUS-CKP", "P-MON-CKGL", "P-JUS-CKGL", "T-JUS-CKGL",
    "P-MON-SDV-P", "PT-P", "T-MON-CKP", "T-PDG", "QGL", "QBS",
]

# ---------------------------------------------------------------------------
# Tópicos Kafka
# ---------------------------------------------------------------------------
KAFKA_SOURCE_TOPIC: str = os.getenv("KAFKA_TOPIC", "3w-sensors")
KAFKA_SINK_TOPIC: str = os.getenv("KAFKA_RESULTS_TOPIC", "3w-results")

# ---------------------------------------------------------------------------
# Parâmetros de janela
# ---------------------------------------------------------------------------
WINDOW_SIZE_SECONDS: int = 60
WATERMARK_MAX_OUT_OF_ORDER_SECONDS: int = 5
STATE_CLEANUP_TIMER_SECONDS: int = 1800   # 30 min sem dados → limpa estado

# ---------------------------------------------------------------------------
# Configuração principal do Flink
# ---------------------------------------------------------------------------
FLINK_CONFIG: dict[str, str] = {
    # State backend
    "state.backend":                                    "rocksdb",
    "state.backend.incremental":                        "true",
    "state.checkpoints.dir":                            "file:///tmp/flink-checkpoints",

    # Checkpointing
    "execution.checkpointing.interval":                 "10s",
    "execution.checkpointing.mode":                     "EXACTLY_ONCE",
    "execution.checkpointing.timeout":                  "60s",
    "execution.checkpointing.max-concurrent-checkpoints": "1",

    # Paralelismo (ajustável por experimento via CLI)
    "parallelism.default": os.getenv("FLINK_PARALLELISM", "4"),

    # Kafka consumer
    "kafka.bootstrap.servers":  os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "kafka.group.id":           "flink-3w-consumer",
    "kafka.auto.offset.reset":  "earliest",

    # Memória e rede
    "taskmanager.network.memory.fraction": "0.1",
    "taskmanager.memory.process.size":     "2048m",
}

# Host/porta do Flink JobManager (para coleta de métricas externas)
FLINK_JM_HOST: str = os.getenv("FLINK_JM_HOST", "localhost")
FLINK_JM_PORT: int = int(os.getenv("FLINK_JM_PORT", "8081"))
