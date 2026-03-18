"""
Job Spark Structured Streaming para o benchmark 3W da Petrobras.

Pipeline:
    KafkaSource (3w-sensors) → parse JSON → watermark + window(60s, 1s) por well_id
    → mean/std features → KafkaSink (3w-results)

Uso:
    python -m spark_pipeline.pipeline
    python -m spark_pipeline.pipeline --duration 600
"""
import argparse
import sys
import threading
import time

import structlog
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from spark_pipeline.config import (
    CHECKPOINT_LOCATION,
    KAFKA_BROKERS,
    PUSHGATEWAY_URL,
    RESULTS_TOPIC,
    SOURCE_TOPIC,
    SPARK_CONFIG,
    TRIGGER_INTERVAL,
)
from spark_pipeline.transformations import (
    add_watermark_and_window,
    calculate_features,
    parse_kafka_messages,
    prepare_sink_output,
)

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Métricas Prometheus
# ---------------------------------------------------------------------------
_registry = CollectorRegistry()

input_rows_per_second = Gauge(
    "spark_streaming_inputRowsPerSecond",
    "Linhas de entrada por segundo no Spark SS",
    ["scenario"],
    registry=_registry,
)
processed_rows_per_second = Gauge(
    "spark_streaming_processedRowsPerSecond",
    "Linhas processadas por segundo no Spark SS",
    ["scenario"],
    registry=_registry,
)
batch_duration_ms = Gauge(
    "spark_streaming_batchDuration",
    "Duração de cada micro-batch em ms",
    ["scenario"],
    registry=_registry,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark SS job para benchmark 3W")
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Duração em segundos (0 = infinito até cancelamento)",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default="manual",
        help="Nome do cenário (usado nas métricas Prometheus)",
    )
    parser.add_argument(
        "--checkpoint-location",
        type=str,
        default=CHECKPOINT_LOCATION,
        help="Localização do checkpoint do Spark SS",
    )
    return parser.parse_args()


def build_spark_session():
    """Cria SparkSession com todas as configurações do benchmark."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder
    for key, value in SPARK_CONFIG.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


class _MetricsListener:
    """
    Listener Spark para coleta de métricas de streaming.
    Enviada via push gateway a cada micro-batch.
    """

    def __init__(self, scenario: str) -> None:
        self.scenario = scenario

    def onQueryProgress(self, event) -> None:
        """Chamado ao fim de cada micro-batch."""
        progress = event.progress
        try:
            input_rows_per_second.labels(scenario=self.scenario).set(
                progress.inputRowsPerSecond or 0.0
            )
            processed_rows_per_second.labels(scenario=self.scenario).set(
                progress.processedRowsPerSecond or 0.0
            )
            batch_duration_ms.labels(scenario=self.scenario).set(
                progress.durationMs.get("triggerExecution", 0) or 0
            )
            push_to_gateway(
                PUSHGATEWAY_URL,
                job="3w_spark_pipeline",
                registry=_registry,
                grouping_key={"scenario": self.scenario},
            )
        except Exception as exc:
            logger.warning("erro_push_gateway", error=str(exc))


def main() -> None:
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
        logger_factory=structlog.PrintLoggerFactory(),
    )

    args = parse_args()
    logger.info("iniciando_pipeline_spark_ss", scenario=args.scenario)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Registrar listener de métricas
    spark.streams.addListener(_MetricsListener(scenario=args.scenario))

    # -----------------------------------------------------------------------
    # Pipeline de streaming
    # -----------------------------------------------------------------------
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.group.id", "spark-3w-consumer")
        .load()
    )

    parsed = parse_kafka_messages(raw_stream)
    grouped = add_watermark_and_window(parsed)
    features = calculate_features(grouped)
    output = prepare_sink_output(features)

    # -----------------------------------------------------------------------
    # Sink Kafka
    # -----------------------------------------------------------------------
    query = (
        output.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("topic", RESULTS_TOPIC)
        .option("checkpointLocation", args.checkpoint_location)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("update")
        .start()
    )

    logger.info(
        "query_iniciada",
        source_topic=SOURCE_TOPIC,
        sink_topic=RESULTS_TOPIC,
        trigger=TRIGGER_INTERVAL,
    )

    # Timeout opcional
    if args.duration > 0:
        try:
            query.awaitTermination(timeout=args.duration)
        except Exception:
            pass
        finally:
            query.stop()
            logger.info("pipeline_encerrado_por_duracao", duration_s=args.duration)
    else:
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("pipeline_interrompido_pelo_usuario")
            query.stop()

    spark.stop()


if __name__ == "__main__":
    main()
