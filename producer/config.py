"""
Configurações do Kafka Producer para o benchmark 3W.
Todos os valores são carregados de variáveis de ambiente com defaults razoáveis.
"""
import os
from dataclasses import dataclass, field
from typing import List


# ---------------------------------------------------------------------------
# Configurações do Kafka Producer
# ---------------------------------------------------------------------------
PRODUCER_CONFIG: dict = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "localhost:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "3w-sensors"),
    "acks": "1",                                    # balanço latência/durabilidade
    "compression_type": "lz4",                     # compressão eficiente para JSON numérico
    "batch_size": 16384,                            # 16 KB
    "linger_ms": 5,                                 # aguarda 5 ms para batching
    "max_in_flight_requests_per_connection": 5,
}

# ---------------------------------------------------------------------------
# Configurações de Replay do Dataset 3W
# ---------------------------------------------------------------------------
REPLAY_CONFIG: dict = {
    "dataset_path": os.getenv("DATASET_PATH", "/data/3w/dataset"),
    "speed_factor": float(os.getenv("SPEED_FACTOR", "1.0")),
    "event_codes": [0, 1, 2, 3, 4, 5, 6, 7, 8],
    "sources": ["REAL", "SIMULATED"],
    "max_concurrent_wells": int(os.getenv("MAX_CONCURRENT_WELLS", "42")),
    "loop": True,                                   # reinicia ao terminar
    "add_producer_timestamp": True,                 # essencial para medir latência
}

# ---------------------------------------------------------------------------
# Métricas
# ---------------------------------------------------------------------------
PUSHGATEWAY_URL: str = os.getenv("PUSHGATEWAY_URL", "http://localhost:9091")
METRICS_PUSH_INTERVAL_S: int = int(os.getenv("METRICS_PUSH_INTERVAL_S", "10"))
