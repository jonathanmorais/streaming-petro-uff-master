"""
Transformações Spark SQL para dados do dataset 3W.

Inclui schema, parsing de mensagens Kafka, watermark/window e cálculo de features.
As transformações são funcionalmente equivalentes ao WellStreamOperator do Flink
para garantir comparação justa no benchmark.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_pipeline.config import SENSOR_FIELDS, WATERMARK_DELAY, WINDOW_SIZE, SLIDE_DURATION


# ---------------------------------------------------------------------------
# Schema da mensagem Kafka
# ---------------------------------------------------------------------------
def get_message_schema() -> StructType:
    """
    Retorna o schema Spark para mensagens JSON publicadas pelo producer 3W.

    Todos os campos de sensores são DoubleType nullable=True para lidar
    com o alto percentual de NaN (~30-40%) do dataset 3W real.
    """
    sensor_fields = [
        StructField(name, DoubleType(), nullable=True)
        for name in SENSOR_FIELDS
    ]

    return StructType(
        [
            StructField("well_id",            StringType(),  nullable=False),
            StructField("source",             StringType(),  nullable=True),
            StructField("event_code",         IntegerType(), nullable=True),
            StructField("timestamp",          StringType(),  nullable=True),
            StructField("timestamp_ms",       LongType(),    nullable=True),
            StructField("producer_ts_ms",     LongType(),    nullable=True),
            *sensor_fields,
            StructField("class",              IntegerType(), nullable=True),
            StructField("state",              IntegerType(), nullable=True),
            StructField("instance_progress",  DoubleType(),  nullable=True),
        ]
    )


# ---------------------------------------------------------------------------
# Parsing de mensagens Kafka
# ---------------------------------------------------------------------------
def parse_kafka_messages(df: DataFrame) -> DataFrame:
    """
    Faz o parse do campo 'value' (bytes) do Kafka para DataFrame estruturado.

    Args:
        df: DataFrame raw do Kafka com colunas (key, value, topic, partition, ...).

    Returns:
        DataFrame com todas as colunas do schema da mensagem + event_timestamp.
    """
    schema = get_message_schema()

    parsed = df.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Converter timestamp_ms (unix ms) para TimestampType
    parsed = parsed.withColumn(
        "event_timestamp",
        (F.col("timestamp_ms") / 1000).cast(TimestampType()),
    )

    return parsed


# ---------------------------------------------------------------------------
# Watermark + Janela deslizante
# ---------------------------------------------------------------------------
def add_watermark_and_window(df: DataFrame) -> DataFrame:
    """
    Adiciona watermark e agrupa por janela deslizante de 60s por poço.

    Equivalente funcional ao keyBy(well_id) + WellStreamOperator do Flink.

    Args:
        df: DataFrame com coluna event_timestamp e well_id.

    Returns:
        DataFrame agrupado por (window, well_id).
    """
    return (
        df.withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            F.window("event_timestamp", WINDOW_SIZE, SLIDE_DURATION),
            F.col("well_id"),
            F.col("event_code"),
        )
    )


# ---------------------------------------------------------------------------
# Cálculo de features
# ---------------------------------------------------------------------------
def calculate_features(grouped_df) -> DataFrame:
    """
    Calcula agregações das variáveis de sensor na janela temporal.

    Calcula mean e stddev das variáveis de pressão/temperatura principais,
    além da latência fim-a-fim. Equivalente ao WellStreamOperator do Flink.

    Args:
        grouped_df: GroupedData resultante de add_watermark_and_window().

    Returns:
        DataFrame com features calculadas e métricas de latência.
    """
    feature_vars = ["P-PDG", "P-TPT", "T-JUS-CKP", "T-TPT", "QGL"]

    agg_exprs = []

    # Features por variável de sensor
    for var in feature_vars:
        safe_name = var.replace("-", "_")
        agg_exprs.extend([
            F.mean(F.col(f"`{var}`")).alias(f"{safe_name}_mean"),
            F.stddev(F.col(f"`{var}`")).alias(f"{safe_name}_std"),
        ])

    # Contagem de registros na janela
    agg_exprs.append(F.count("*").alias("window_record_count"))

    # Contagem de eventos anômalos (class != 0)
    agg_exprs.append(
        F.sum(F.when(F.col("class") != 0, 1).otherwise(0)).alias("anomaly_count")
    )

    # Latência fim-a-fim: tempo atual - producer_ts_ms
    # consumer_ts_ms = unix_timestamp() * 1000 (em ms)
    consumer_ts_ms_expr = (F.unix_timestamp() * 1000).cast(LongType())
    agg_exprs.extend([
        F.first("producer_ts_ms").alias("producer_ts_ms"),
        (consumer_ts_ms_expr - F.first("producer_ts_ms")).alias("e2e_latency_ms"),
        consumer_ts_ms_expr.alias("consumer_ts_ms"),
    ])

    return grouped_df.agg(*agg_exprs)


# ---------------------------------------------------------------------------
# Preparação do sink Kafka
# ---------------------------------------------------------------------------
def prepare_sink_output(df: DataFrame) -> DataFrame:
    """
    Formata o resultado para publicação no tópico Kafka de resultados.

    Args:
        df: DataFrame com features calculadas.

    Returns:
        DataFrame com colunas 'key' (well_id bytes) e 'value' (JSON bytes).
    """
    # Selecionar colunas de saída (excluindo a struct 'window' que não é serializável diretamente)
    output_cols = [
        F.col("well_id"),
        F.col("event_code"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("window_record_count"),
        F.col("anomaly_count"),
        F.col("producer_ts_ms"),
        F.col("consumer_ts_ms"),
        F.col("e2e_latency_ms"),
    ]

    # Adicionar colunas de features
    feature_vars = ["P_PDG", "P_TPT", "T_JUS_CKP", "T_TPT", "QGL"]
    for var in feature_vars:
        output_cols.extend([
            F.col(f"{var}_mean"),
            F.col(f"{var}_std"),
        ])

    result = df.select(*output_cols)

    # Serializar para JSON como valor Kafka
    value_expr = F.to_json(F.struct(*[c._jc.toString() for c in output_cols]))  # type: ignore

    return result.select(
        F.col("well_id").cast("string").alias("key"),
        F.to_json(F.struct(*result.columns)).alias("value"),
    )
