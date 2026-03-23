#!/usr/bin/env python3
"""
Spark Structured Streaming benchmark — dataset 3W
Adaptado para rodar em cluster mode via Spark Operator no EKS.

Diferenças em relação ao run.py (local mode):
  - Sem .master("local[*]") — o Spark Operator injeta o master do cluster
  - Dataset lido do S3 via Spark (não pandas) usando s3a://
  - Dados de staging gravados no S3 (não em tmpdir local)
  - Resultados e métricas salvos no S3

Variáveis de ambiente (obrigatórias):
  DATASET_PATH   s3a://seu-bucket-3w/3w/dataset
  OUTPUT_PATH    s3a://seu-bucket-3w/results/spark

Variáveis de ambiente (opcionais):
  MAX_ROWS       100000   (0 = sem limite)
  MAX_FILES      5        (arquivos parquet por classe de evento)
"""
from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

import numpy as np

# ---------------------------------------------------------------------------
# Config via env vars
# ---------------------------------------------------------------------------
DATASET_PATH = os.environ.get("DATASET_PATH")
OUTPUT_PATH  = os.environ.get("OUTPUT_PATH")

if not DATASET_PATH or not OUTPUT_PATH:
    print("[ERRO] DATASET_PATH e OUTPUT_PATH são obrigatórios.")
    print("  export DATASET_PATH=s3a://seu-bucket-3w/3w/dataset")
    print("  export OUTPUT_PATH=s3a://seu-bucket-3w/results/spark")
    sys.exit(1)

MAX_ROWS  = int(os.environ.get("MAX_ROWS", "100000"))
MAX_FILES = int(os.environ.get("MAX_FILES", "5"))

STAGING_PATH = f"{OUTPUT_PATH}/_staging"
RESULTS_PATH = f"{OUTPUT_PATH}/stream_results"

SENSOR_COLS = [
    "P-PDG", "P-TPT", "T-TPT", "P-MON-CKP", "T-JUS-CKP",
    "P-JUS-CKP", "P-MON-CKGL", "P-JUS-CKGL", "QGL", "QBS",
]


# ---------------------------------------------------------------------------
# Spark Session (sem .master() — cluster mode gerenciado pelo Operator)
# ---------------------------------------------------------------------------
def build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName("3W-Benchmark-Spark")
        .config("spark.sql.shuffle.partitions", "8")
        # S3A via IRSA (credenciais injetadas pelo IRSA/WebIdentity no EKS)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.WebIdentityTokenFileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Checkpointing no S3
        .config("spark.sql.streaming.checkpointLocation",
                f"{OUTPUT_PATH}/_checkpoint")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# 1. Carrega dataset do S3 com Spark (substitui pandas + iteração local)
# ---------------------------------------------------------------------------
def load_dataset_to_staging(spark) -> int:
    """
    Lê os parquets do dataset 3W no S3, faz amostragem se MAX_ROWS > 0,
    adiciona producer_ts_ms e grava em STAGING_PATH para o streaming ler.

    Retorna o número de registros gravados.
    """
    import pyspark.sql.functions as F

    print(f"[1/3] Lendo dataset de {DATASET_PATH} ...")

    # Lê todos os subdiretórios 0-9 de uma vez
    df = spark.read.parquet(DATASET_PATH)

    # Garante coluna timestamp_ms
    if "timestamp_ms" not in df.columns:
        df = df.withColumn(
            "timestamp_ms",
            (F.col("timestamp").cast("long") / 1_000).cast("long"),
        )

    # Adiciona producer_ts_ms (momento de "ingestão" simulado)
    ingestion_ts = int(time.time() * 1000)
    df = df.withColumn("producer_ts_ms", F.lit(ingestion_ts))

    # Amostragem (replica o comportamento do run.py)
    total = df.count()
    print(f"  Total de registros no dataset: {total:,}")

    if MAX_ROWS > 0 and total > MAX_ROWS:
        fraction = MAX_ROWS / total
        df = df.sample(fraction=fraction, seed=42)
        sampled = df.count()
        print(f"  Amostra: {sampled:,} registros ({MAX_ROWS} solicitados)")
    else:
        print(f"  Usando todos os {total:,} registros (sem limite)")

    # Grava staging no S3 (reparticionado para o streaming processar em batches)
    n_partitions = max(1, min(20, (MAX_ROWS or total) // 5000))
    print(f"  Gravando staging em {STAGING_PATH} ({n_partitions} partições)...")
    df.repartition(n_partitions).write.mode("overwrite").parquet(STAGING_PATH)

    return df.count()


# ---------------------------------------------------------------------------
# 2. Pipeline de Structured Streaming com janela de 60s
# ---------------------------------------------------------------------------
def run_streaming(spark, total_records: int) -> dict[str, Any]:
    import pyspark.sql.functions as F

    print(f"\n[2/3] Iniciando Structured Streaming ...")

    schema = spark.read.parquet(STAGING_PATH).schema
    available = [c for c in SENSOR_COLS if c in schema.fieldNames()]

    agg_exprs = [
        F.avg(c).alias(f"avg_{c.lower().replace('-', '_')}")
        for c in available
    ] + [
        F.stddev(c).alias(f"std_{c.lower().replace('-', '_')}")
        for c in available
    ]

    sdf = spark.readStream.format("parquet").schema(schema).load(STAGING_PATH)

    result_sdf = (
        sdf
        .withColumn("event_time",
                    (F.col("timestamp_ms") / 1000).cast("timestamp"))
        .withWatermark("event_time", "10 seconds")
        .groupBy(
            F.window("event_time", "60 seconds"),
            "well_id",
            "event_code",
        )
        .agg(
            F.count("*").alias("record_count"),
            F.max("producer_ts_ms").alias("max_producer_ts_ms"),
            *agg_exprs,
        )
        .withColumn("processing_ts_ms",
                    (F.unix_timestamp() * 1000).cast("long"))
        .withColumn("latency_ms",
                    F.col("processing_ts_ms") - F.col("max_producer_ts_ms"))
    )

    t_start = time.time()

    # Grava resultados no S3 e também em memória para coletar métricas
    query_s3 = (
        result_sdf.writeStream
        .format("parquet")
        .option("path", RESULTS_PATH)
        .trigger(once=True)
        .start()
    )

    query_mem = (
        result_sdf.writeStream
        .format("memory")
        .queryName("spark_results")
        .trigger(once=True)
        .start()
    )

    query_s3.awaitTermination(timeout=600)
    query_mem.awaitTermination(timeout=600)

    t_end = time.time()
    total_time_s = t_end - t_start

    # Coleta métricas da query em memória
    progress = query_mem.lastProgress or {}
    num_input_rows = progress.get("numInputRows", total_records)
    duration_ms = progress.get("durationMs", {})

    results_rows = spark.sql(
        "SELECT latency_ms, record_count FROM spark_results "
        "WHERE latency_ms IS NOT NULL"
    ).collect()

    latencies = [r["latency_ms"] for r in results_rows if r["latency_ms"] is not None]
    window_sizes = [r["record_count"] for r in results_rows if r["record_count"] is not None]

    throughput = num_input_rows / total_time_s if total_time_s > 0 else 0.0

    metrics: dict[str, Any] = {
        "engine": "spark",
        "total_time_s": round(total_time_s, 3),
        "total_records": total_records,
        "windows_processed": len(latencies),
        "throughput_rps": round(throughput, 2),
        "input_rows_per_second": progress.get("inputRowsPerSecond", throughput),
        "processed_rows_per_second": progress.get("processedRowsPerSecond", throughput),
        "trigger_execution_ms": duration_ms.get("triggerExecution", 0),
        "latency_p50": float(np.percentile(latencies, 50)) if latencies else 0.0,
        "latency_p95": float(np.percentile(latencies, 95)) if latencies else 0.0,
        "latency_p99": float(np.percentile(latencies, 99)) if latencies else 0.0,
        "latency_mean": float(np.mean(latencies)) if latencies else 0.0,
        "latency_ms_sample": latencies[:2000],
        "window_sizes": window_sizes,
    }

    print(f"\n  Tempo     : {total_time_s:.2f}s")
    print(f"  Throughput: {throughput:.0f} rec/s")
    print(f"  Janelas   : {len(latencies)}")
    if latencies:
        print(f"  Latência  : p50={metrics['latency_p50']:.0f}ms  "
              f"p95={metrics['latency_p95']:.0f}ms  "
              f"p99={metrics['latency_p99']:.0f}ms")

    return metrics


# ---------------------------------------------------------------------------
# 3. Salva métricas no S3
# ---------------------------------------------------------------------------
def save_metrics(spark, metrics: dict[str, Any]) -> None:
    import pyspark.sql.functions as F

    print(f"\n[3/3] Salvando métricas em {OUTPUT_PATH}/metrics.json ...")

    to_save = {k: v for k, v in metrics.items() if k != "latency_ms_sample"}
    to_save["latency_ms_sample"] = metrics.get("latency_ms_sample", [])

    json_str = json.dumps(to_save, indent=2)

    # Grava via Spark como arquivo de texto no S3
    spark.sparkContext \
        .parallelize([json_str], 1) \
        .saveAsTextFile(f"{OUTPUT_PATH}/metrics_raw")

    print("  Métricas salvas.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print("=" * 60)
    print("  Benchmark 3W — Spark Structured Streaming (EKS)")
    print(f"  DATASET_PATH : {DATASET_PATH}")
    print(f"  OUTPUT_PATH  : {OUTPUT_PATH}")
    print(f"  MAX_ROWS     : {'sem limite' if MAX_ROWS == 0 else f'{MAX_ROWS:,}'}")
    print("=" * 60)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    total = load_dataset_to_staging(spark)
    metrics = run_streaming(spark, total)
    save_metrics(spark, metrics)

    print("\n" + "=" * 60)
    print("  CONCLUÍDO")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
