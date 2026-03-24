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

STAGING_PATH = f"{OUTPUT_PATH}/staging"
RESULTS_PATH = f"{OUTPUT_PATH}/stream_results"

SENSOR_COLS = [
    "P-PDG", "P-TPT", "T-TPT", "P-MON-CKP", "T-JUS-CKP",
    "P-JUS-CKP", "P-MON-CKGL", "P-JUS-CKGL", "QGL", "QBS",
]

# Schema explícito do dataset 3W v2.
# O índice DatetimeIndex do pandas é gravado como INT64 TIMESTAMP(NANOS) no Parquet.
# Spark rejeita NANOS por padrão; ao fornecer LongType explicitamente
# o Spark lê os bytes INT64 diretamente sem passar pelo conversor de tipos.
def schema_3w():
    from pyspark.sql.types import StructType, StructField, LongType, DoubleType
    fields = [StructField("timestamp", LongType(), True)]
    fields += [StructField(c, DoubleType(), True) for c in SENSOR_COLS]
    fields += [StructField("class", DoubleType(), True)]
    return StructType(fields)


# ---------------------------------------------------------------------------
# Spark Session (sem .master() — cluster mode gerenciado pelo Operator)
# ---------------------------------------------------------------------------
def build_spark():
    from pyspark.sql import SparkSession

    # Em cluster mode, .getOrCreate() devolve a sessão já criada pelo spark-submit
    # e ignora os .config() do builder. Usar spark.conf.set() após obter a sessão
    # é o único modo confiável de alterar SQLConf em cluster mode.
    spark = SparkSession.builder.appName("3W-Benchmark-Spark").getOrCreate()

    # --- NANOS fix (três camadas para garantir cobertura total) ---
    # 1. SQLConf do driver: usado na inferência de schema (driver-side).
    spark.conf.set("spark.sql.parquet.nanosAsLong", "true")

    # 2. HadoopConf do SparkContext: copiado por newHadoopConf() ao construir
    #    o broadcast conf que os executors recebem. O leitor row-based lê
    #    nanosAsLong diretamente deste conf (ParquetToSparkSchemaConverter(conf)).
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "spark.sql.parquet.nanosAsLong", "true")

    # 3. Força leitor row-based: o leitor vetorizado lê nanosAsLong de SQLConf.get
    #    nos executors, que não herda a sessão do driver. O row-based lê do
    #    HadoopConf (item 2 acima), que chega corretamente aos executors.
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    spark.conf.set("spark.sql.streaming.checkpointLocation",
                   f"{OUTPUT_PATH}/_checkpoint")
    return spark


# ---------------------------------------------------------------------------
# 1. Carrega dataset do S3 com boto3+pyarrow (bypassa o leitor Parquet do Spark)
# ---------------------------------------------------------------------------
def load_dataset_to_staging(spark) -> int:
    """
    Lê os parquets do dataset 3W diretamente do S3 usando boto3+pyarrow,
    normaliza TIMESTAMP_NANOS→int64 e class INT32→float64 em Python puro,
    converte para Spark DataFrame e grava em STAGING_PATH.

    Por que boto3+pyarrow em vez de spark.read.parquet():
      O dataset 3W contém TIMESTAMP(NANOS,false) que o Spark 3.5.3 não lê
      nos executors — nanosAsLong=true é session-local no driver e não é
      herdado pelos SQLConf dos executors. pyarrow lida com NANOS nativamente.

    Retorna o número de registros gravados.
    """
    import io
    import random
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd

    print(f"[1/3] Lendo dataset de {DATASET_PATH} via boto3+pyarrow ...")

    # Extrai bucket e prefix do caminho s3a://bucket/prefix
    s3_path = DATASET_PATH.replace("s3a://", "").replace("s3://", "")
    bucket_name = s3_path.split("/")[0]
    prefix = "/".join(s3_path.split("/")[1:])
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    s3 = boto3.client("s3")

    # Lista todos os arquivos .parquet recursivamente
    parquet_keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                parquet_keys.append(obj["Key"])

    print(f"  Arquivos parquet encontrados: {len(parquet_keys)}")
    if not parquet_keys:
        print("[ERRO] Nenhum arquivo parquet encontrado em DATASET_PATH.")
        sys.exit(1)

    # Limita arquivos para evitar OOM no driver (MAX_FILES * 20 como teto)
    if MAX_FILES > 0 and len(parquet_keys) > MAX_FILES * 20:
        random.seed(42)
        random.shuffle(parquet_keys)
        parquet_keys = parquet_keys[: MAX_FILES * 20]
        print(f"  Limitado a {len(parquet_keys)} arquivos (MAX_FILES={MAX_FILES})")

    # Lê e normaliza cada arquivo com pyarrow
    frames: list[pd.DataFrame] = []
    errors = 0
    for key in parquet_keys:
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=key)
            buf = io.BytesIO(obj["Body"].read())
            table = pq.read_table(buf)

            cols = {}
            for field in table.schema:
                col = table.column(field.name)
                if field.name == "timestamp" and pa.types.is_timestamp(col.type):
                    # TIMESTAMP_NANOS → int64 (nanosegundos)
                    cols[field.name] = col.cast(pa.int64())
                elif field.name == "class":
                    # INT32 ou DOUBLE → float64 uniforme
                    cols[field.name] = col.cast(pa.float64())
                else:
                    cols[field.name] = col

            frames.append(pa.table(cols).to_pandas())
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  [WARN] Pulando {key}: {e}")

    if not frames:
        print("[ERRO] Nenhum arquivo lido com sucesso.")
        sys.exit(1)

    print(f"  Arquivos lidos: {len(frames)}  (erros ignorados: {errors})")

    df_pd = pd.concat(frames, ignore_index=True)
    total = len(df_pd)
    print(f"  Total de registros: {total:,}")

    # Amostragem
    if MAX_ROWS > 0 and total > MAX_ROWS:
        df_pd = df_pd.sample(n=MAX_ROWS, random_state=42).reset_index(drop=True)
        print(f"  Amostra: {len(df_pd):,} registros ({MAX_ROWS} solicitados)")
    else:
        print(f"  Usando todos os {total:,} registros (sem limite)")

    # Adiciona colunas derivadas
    if "timestamp_ms" not in df_pd.columns:
        df_pd["timestamp_ms"] = (df_pd["timestamp"] // 1_000_000).astype("int64")

    df_pd["producer_ts_ms"] = int(time.time() * 1000)

    # Garante float64 nas colunas de sensor
    for c in SENSOR_COLS:
        if c in df_pd.columns:
            df_pd[c] = df_pd[c].astype("float64")

    # Converte para Spark DataFrame e grava staging no S3
    print(f"  Convertendo para Spark e gravando em {STAGING_PATH} ...")
    n_records = len(df_pd)
    n_partitions = max(1, min(20, n_records // 5000))
    spark.createDataFrame(df_pd).repartition(n_partitions).write.mode("overwrite").parquet(STAGING_PATH)
    print(f"  Staging gravado: {n_records:,} registros em {n_partitions} partições")

    return n_records


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
            "class",
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

    # outputMode("complete"): emite todas as janelas do estado em cada batch.
    # Necessário com trigger(once=True): em "append" mode, o watermark inicia em
    # epoch-zero e só avança no batch seguinte — com um único batch nada é emitido.
    # "complete" mode emite tudo independente do watermark.
    # Nota: parquet sink não suporta "complete"; os resultados são coletados via
    # memory sink e depois gravados no S3 como batch write.
    query_mem = (
        result_sdf.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("spark_results")
        .trigger(once=True)
        .start()
    )

    query_mem.awaitTermination(timeout=600)

    t_end = time.time()
    total_time_s = t_end - t_start

    # Coleta métricas da query em memória
    progress = query_mem.lastProgress or {}
    num_input_rows = progress.get("numInputRows", total_records)
    duration_ms = progress.get("durationMs", {})

    results_df = spark.sql("SELECT * FROM spark_results")

    # Grava resultados no S3 como batch write (parquet streaming não suporta complete mode)
    results_df.write.mode("overwrite").parquet(RESULTS_PATH)

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
def save_metrics(metrics: dict[str, Any]) -> None:
    import boto3

    print(f"\n[3/3] Salvando métricas em {OUTPUT_PATH}/metrics.json ...")

    to_save = {k: v for k, v in metrics.items() if k != "latency_ms_sample"}
    to_save["latency_ms_sample"] = metrics.get("latency_ms_sample", [])

    json_str = json.dumps(to_save, indent=2)

    # Extrai bucket e key do caminho s3a://bucket/prefix/...
    path = OUTPUT_PATH.replace("s3a://", "").replace("s3://", "")
    bucket = path.split("/")[0]
    key    = "/".join(path.split("/")[1:]) + "/metrics.json"

    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=json_str.encode())
    print(f"  Métricas salvas em s3://{bucket}/{key}")


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
    save_metrics(metrics)

    print("\n" + "=" * 60)
    print("  CONCLUÍDO")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
