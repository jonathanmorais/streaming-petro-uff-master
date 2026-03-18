"""
Job PyFlink para o benchmark 3W da Petrobras.

Pipeline:
    KafkaSource (3w-sensors) → Deserialize JSON → keyBy(well_id)
    → WellStreamOperator → Serialize JSON → KafkaSink (3w-results)

Uso:
    python -m flink_pipeline.pipeline --parallelism 4
    python -m flink_pipeline.pipeline --parallelism 2 --duration 600
"""
import argparse
import json
import sys
import time

import structlog

from flink_pipeline.config import (
    FLINK_CONFIG,
    KAFKA_SINK_TOPIC,
    KAFKA_SOURCE_TOPIC,
    WATERMARK_MAX_OUT_OF_ORDER_SECONDS,
)
from flink_pipeline.operators import WellStreamOperator

logger = structlog.get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flink job para benchmark 3W")
    parser.add_argument(
        "--parallelism",
        type=int,
        default=int(FLINK_CONFIG["parallelism.default"]),
        help="Paralelismo do job Flink",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Duração em segundos (0 = infinito até cancelamento)",
    )
    return parser.parse_args()


def build_pipeline(parallelism: int) -> None:
    """
    Constrói e executa o pipeline Flink para o benchmark 3W.

    Args:
        parallelism: Grau de paralelismo do job.
    """
    from pyflink.common import WatermarkStrategy, Duration, SimpleStringSchema
    from pyflink.common.typeinfo import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
        KafkaSource,
        KafkaSink,
        KafkaRecordSerializationSchema,
        DeliveryGuarantee,
    )
    from pyflink.datastream.formats.json import JsonRowDeserializationSchema

    # -----------------------------------------------------------------------
    # 1. Criar ambiente de execução
    # -----------------------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)

    # Aplicar configurações do Flink
    cfg = env.get_config()
    for key, value in FLINK_CONFIG.items():
        env.get_checkpoint_config()  # força inicialização
        try:
            cfg.set_global_job_parameter(key, str(value))
        except Exception:
            pass

    # Configurar checkpointing EXACTLY_ONCE
    env.enable_checkpointing(10_000)  # 10 segundos
    checkpoint_cfg = env.get_checkpoint_config()
    from pyflink.datastream import CheckpointingMode
    checkpoint_cfg.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    checkpoint_cfg.set_checkpoint_timeout(60_000)
    checkpoint_cfg.set_max_concurrent_checkpoints(1)

    kafka_brokers = FLINK_CONFIG["kafka.bootstrap.servers"]

    # -----------------------------------------------------------------------
    # 2. KafkaSource com event-time e watermarks
    # -----------------------------------------------------------------------
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_brokers)
        .set_topics(KAFKA_SOURCE_TOPIC)
        .set_group_id(FLINK_CONFIG["kafka.group.id"])
        .set_starting_offsets(
            __import__(
                "pyflink.datastream.connectors.kafka",
                fromlist=["KafkaOffsetsInitializer"],
            ).KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(WATERMARK_MAX_OUT_OF_ORDER_SECONDS)
        )
        .with_timestamp_assigner(
            # Extrai timestamp_ms do JSON como event-time
            lambda event, _: _extract_timestamp_ms(event)
        )
    )

    # -----------------------------------------------------------------------
    # 3. Stream principal
    # -----------------------------------------------------------------------
    stream = (
        env.from_source(
            source=kafka_source,
            watermark_strategy=watermark_strategy,
            source_name="KafkaSource-3w-sensors",
        )
        .map(
            lambda raw: _parse_json(raw),
            output_type=Types.MAP(Types.STRING(), Types.STRING()),
        )
        .filter(lambda msg: msg is not None)
        .key_by(lambda msg: msg.get("well_id", "unknown"))
        .process(WellStreamOperator())
        .map(
            lambda result: json.dumps(result, default=str),
            output_type=Types.STRING(),
        )
    )

    # -----------------------------------------------------------------------
    # 4. KafkaSink para tópico de resultados
    # -----------------------------------------------------------------------
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka_brokers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KAFKA_SINK_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .set_transactional_id_prefix("flink-3w-")
        .build()
    )

    stream.sink_to(kafka_sink)

    # -----------------------------------------------------------------------
    # 5. Executar
    # -----------------------------------------------------------------------
    logger.info(
        "executando_job_flink",
        parallelism=parallelism,
        source_topic=KAFKA_SOURCE_TOPIC,
        sink_topic=KAFKA_SINK_TOPIC,
    )
    env.execute("3W-Benchmark-FlinkJob")


def _extract_timestamp_ms(event_json: str) -> int:
    """Extrai campo timestamp_ms do JSON para uso como event-time."""
    try:
        data = json.loads(event_json)
        return int(data.get("timestamp_ms", 0))
    except Exception:
        return 0


def _parse_json(raw: str) -> dict | None:
    """Deserializa mensagem JSON do Kafka."""
    try:
        return json.loads(raw)
    except Exception:
        logger.warning("mensagem_invalida_ignorada", raw=raw[:100])
        return None


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
    logger.info("iniciando_pipeline_flink", parallelism=args.parallelism)

    if args.duration > 0:
        import threading

        stop = threading.Event()

        def _watchdog():
            time.sleep(args.duration)
            logger.info("duracao_atingida")
            stop.set()

        threading.Thread(target=_watchdog, daemon=True).start()

    try:
        build_pipeline(parallelism=args.parallelism)
    except KeyboardInterrupt:
        logger.info("job_interrompido_pelo_usuario")
    except Exception as exc:
        logger.error("erro_no_job_flink", error=str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
