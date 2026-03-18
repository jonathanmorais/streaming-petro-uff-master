"""
Kafka Producer principal para o benchmark 3W da Petrobras.

Publica eventos do dataset 3W no Kafka com controle de velocidade de replay
e coleta de métricas Prometheus via push gateway.

Uso:
    python -m producer.producer --speed-factor 5.0 --max-wells 10 --duration 120
"""
import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from typing import Any

import structlog
from kafka import KafkaProducer
from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway

from producer.config import (
    METRICS_PUSH_INTERVAL_S,
    PRODUCER_CONFIG,
    PUSHGATEWAY_URL,
    REPLAY_CONFIG,
)
from producer.replay import ReplayOrchestrator

# ---------------------------------------------------------------------------
# Configuração de logging
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)
logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Métricas Prometheus
# ---------------------------------------------------------------------------
_registry = CollectorRegistry()

messages_total = Counter(
    "3w_producer_messages_total",
    "Total de mensagens publicadas no Kafka",
    ["well_id", "event_code", "source"],
    registry=_registry,
)
messages_per_second = Gauge(
    "3w_producer_messages_per_second",
    "Taxa atual de publicação de mensagens",
    ["scenario"],
    registry=_registry,
)
kafka_errors_total = Counter(
    "3w_producer_kafka_errors_total",
    "Total de erros de publicação Kafka",
    registry=_registry,
)


def _json_serializer(value: Any) -> bytes:
    """Serializa dicionário Python para bytes JSON."""
    return json.dumps(value, default=str).encode("utf-8")


def _build_kafka_producer(config: dict) -> KafkaProducer:
    """Cria instância do KafkaProducer com as configurações fornecidas."""
    return KafkaProducer(
        bootstrap_servers=config["bootstrap_servers"],
        value_serializer=_json_serializer,
        key_serializer=lambda k: k if isinstance(k, bytes) else k.encode("utf-8"),
        acks=config.get("acks", "1"),
        compression_type=config.get("compression_type", "lz4"),
        batch_size=config.get("batch_size", 16384),
        linger_ms=config.get("linger_ms", 5),
        max_in_flight_requests_per_connection=config.get(
            "max_in_flight_requests_per_connection", 5
        ),
        retries=3,
    )


def _push_metrics_loop(
    scenario: str,
    orchestrator: ReplayOrchestrator,
    stop_event: threading.Event,
    interval_s: int = METRICS_PUSH_INTERVAL_S,
) -> None:
    """Loop de push de métricas para o Prometheus pushgateway."""
    prev_count = 0
    prev_time = time.monotonic()

    while not stop_event.is_set():
        time.sleep(interval_s)
        if stop_event.is_set():
            break

        curr_count = orchestrator.total_messages_sent
        curr_time = time.monotonic()
        elapsed = curr_time - prev_time or 1.0
        rate = (curr_count - prev_count) / elapsed

        messages_per_second.labels(scenario=scenario).set(rate)
        prev_count = curr_count
        prev_time = curr_time

        try:
            push_to_gateway(
                PUSHGATEWAY_URL,
                job="3w_producer",
                registry=_registry,
                grouping_key={"scenario": scenario},
            )
        except Exception as exc:
            logger.warning("erro_push_gateway", error=str(exc))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka Producer para o benchmark 3W da Petrobras"
    )
    parser.add_argument(
        "--speed-factor",
        type=float,
        default=REPLAY_CONFIG["speed_factor"],
        help="Multiplicador de velocidade de replay (1.0 = tempo real)",
    )
    parser.add_argument(
        "--max-wells",
        type=int,
        default=REPLAY_CONFIG["max_concurrent_wells"],
        help="Número máximo de poços simultâneos",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Duração em segundos (0 = infinito)",
    )
    parser.add_argument(
        "--event-codes",
        type=str,
        default=",".join(str(c) for c in REPLAY_CONFIG["event_codes"]),
        help="Códigos de eventos separados por vírgula (ex: 0,1,2)",
    )
    parser.add_argument(
        "--sources",
        type=str,
        default=",".join(REPLAY_CONFIG["sources"]),
        help="Origens separadas por vírgula (REAL,SIMULATED,DRAWN)",
    )
    parser.add_argument(
        "--dataset-path",
        type=str,
        default=REPLAY_CONFIG["dataset_path"],
        help="Caminho local do dataset 3W",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default="manual",
        help="Nome do cenário (usado nas métricas Prometheus)",
    )
    parser.add_argument(
        "--no-loop",
        action="store_true",
        help="Não reiniciar replay ao terminar todas as instâncias",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info(
        "producer_iniciando",
        speed_factor=args.speed_factor,
        max_wells=args.max_wells,
        duration=args.duration,
        scenario=args.scenario,
    )

    # Montar configurações a partir dos args
    replay_cfg = {
        **REPLAY_CONFIG,
        "speed_factor": args.speed_factor,
        "max_concurrent_wells": args.max_wells,
        "event_codes": [int(c) for c in args.event_codes.split(",")],
        "sources": args.sources.split(","),
        "loop": not args.no_loop,
        "topic": PRODUCER_CONFIG["topic"],
    }

    # Criar producer Kafka
    try:
        producer = _build_kafka_producer(PRODUCER_CONFIG)
        logger.info("kafka_producer_criado", brokers=PRODUCER_CONFIG["bootstrap_servers"])
    except Exception as exc:
        logger.error("falha_ao_criar_producer", error=str(exc))
        sys.exit(1)

    # Criar orquestrador
    orchestrator = ReplayOrchestrator(
        dataset_path=args.dataset_path,
        producer=producer,
        config=replay_cfg,
    )

    # Stop event compartilhado
    stop_event = threading.Event()

    # Handler de sinal para encerramento gracioso
    def _signal_handler(sig, frame):
        logger.info("sinal_recebido_encerrando", signal=sig)
        stop_event.set()
        orchestrator.stop()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Thread de métricas
    metrics_thread = threading.Thread(
        target=_push_metrics_loop,
        args=(args.scenario, orchestrator, stop_event),
        daemon=True,
        name="metrics-pusher",
    )
    metrics_thread.start()

    # Thread de controle de duração
    if args.duration > 0:
        def _duration_watchdog():
            time.sleep(args.duration)
            if not stop_event.is_set():
                logger.info("duracao_atingida_encerrando", duration_s=args.duration)
                stop_event.set()
                orchestrator.stop()

        watchdog = threading.Thread(target=_duration_watchdog, daemon=True, name="watchdog")
        watchdog.start()

    # Executar replay (bloqueia até stop)
    orchestrator.run()

    # Flush e fechar producer
    try:
        producer.flush(timeout=30)
        producer.close(timeout=10)
    except Exception as exc:
        logger.warning("erro_ao_fechar_producer", error=str(exc))

    logger.info(
        "producer_encerrado",
        total_messages=orchestrator.total_messages_sent,
    )


if __name__ == "__main__":
    main()
