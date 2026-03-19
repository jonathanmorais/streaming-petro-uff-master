#!/usr/bin/env python3
"""
Benchmark simplificado: Spark vs Flink no dataset 3W (sem Kafka/Docker).

Carrega arquivos parquet do 3W, processa com cada engine e mede
latência e throughput. Salva métricas em JSON para posterior plotagem.

Uso:
  python benchmark_simple/run.py
  python benchmark_simple/run.py --dataset /path/to/3W/dataset --engine spark
  python benchmark_simple/run.py --dataset /path/to/3W/dataset --engine flink
  python benchmark_simple/run.py --dataset /path/to/3W/dataset --max-files 3
"""
import argparse
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# Colunas de sensores disponíveis no dataset 3W v2
SENSOR_COLS = [
    "P-PDG", "P-TPT", "T-TPT", "P-MON-CKP", "T-JUS-CKP",
    "P-JUS-CKP", "P-MON-CKGL", "P-JUS-CKGL", "QGL", "QBS",
]

DEFAULT_DATASET_PATH = os.environ.get("DATASET_PATH", "/home/jonathan/3W/dataset")
DEFAULT_OUTPUT_DIR = Path(__file__).parent / "results"


# ---------------------------------------------------------------------------
# Carregamento do dataset
# ---------------------------------------------------------------------------

def load_3w_dataset(
    dataset_path: str,
    max_files_per_class: int = 5,
    max_rows: int = 0,
) -> pd.DataFrame:
    """
    Varre os diretórios 0-9 do dataset 3W, lê até max_files_per_class
    arquivos parquet por classe de evento e retorna um DataFrame consolidado.

    Args:
        dataset_path: Caminho para o diretório raiz do dataset 3W.
        max_files_per_class: Máximo de arquivos parquet por classe de evento.
        max_rows: Se > 0, amostra aleatória desse número de linhas (0 = sem limite).
    """
    dataset_path = Path(dataset_path)
    dfs: list[pd.DataFrame] = []
    total_files = 0

    for event_dir in sorted(dataset_path.iterdir()):
        if not event_dir.is_dir() or not event_dir.name.isdigit():
            continue
        event_code = int(event_dir.name)

        parquet_files = sorted(event_dir.glob("*.parquet"))[:max_files_per_class]
        for pf in parquet_files:
            try:
                df = pd.read_parquet(pf)
                df.index.name = "timestamp"
                df = df.reset_index()

                # Determina source e well_id a partir do nome do arquivo
                stem = pf.stem
                if stem.startswith("WELL-"):
                    source = "REAL"
                elif stem.startswith("SIMULATED"):
                    source = "SIMULATED"
                elif stem.startswith("DRAWN"):
                    source = "DRAWN"
                else:
                    source = "UNKNOWN"

                df["well_id"] = stem
                df["source"] = source
                df["event_code"] = event_code

                # Converte índice datetime para milissegundos Unix
                df["timestamp_ms"] = (
                    pd.to_datetime(df["timestamp"])
                    .astype("int64") // 1_000_000
                )

                # Converte colunas de sensores para float64 (Int64 → float)
                for col in SENSOR_COLS:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")

                dfs.append(df)
                total_files += 1
            except Exception as exc:
                print(f"  [WARN] Pulando {pf.name}: {exc}")

    if not dfs:
        raise RuntimeError(f"Nenhum arquivo parquet encontrado em {dataset_path}")

    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.sort_values("timestamp_ms").reset_index(drop=True)

    total_loaded = len(combined)
    if max_rows > 0 and total_loaded > max_rows:
        combined = combined.sample(n=max_rows, random_state=42).sort_values("timestamp_ms").reset_index(drop=True)
        print(f"Dataset carregado: {total_files} arquivos | {len(combined):,} linhas (amostrado de {total_loaded:,})")
    else:
        print(f"Dataset carregado: {total_files} arquivos | {len(combined):,} linhas")

    return combined


# ---------------------------------------------------------------------------
# Benchmark Spark
# ---------------------------------------------------------------------------

def run_spark_benchmark(df: pd.DataFrame, output_dir: Path) -> dict[str, Any]:
    """
    Processa o DataFrame com Spark Structured Streaming (trigger once).

    Estratégia:
      1. Escreve o DataFrame em parquet no diretório /tmp para fonte de streaming.
      2. Configura readStream.format("parquet") + watermark + window 60s.
      3. Usa trigger(once=True) para processar todos os dados de uma vez.
      4. Coleta métricas de lastProgress e latência por janela.
    """
    try:
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as F
    except ImportError:
        print("[ERRO] PySpark não encontrado. Instale: pip install pyspark")
        return {}

    tmpdir = tempfile.mkdtemp(prefix="3w_spark_")
    try:
        spark = (
            SparkSession.builder
            .appName("3W-Benchmark-Spark")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "1g")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.streaming.checkpointLocation", f"{tmpdir}/checkpoint")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        # Define producer_ts_ms agora (antes de iniciar o streaming)
        ingestion_ts_ms = int(time.time() * 1000)
        df = df.copy()
        df["producer_ts_ms"] = ingestion_ts_ms

        # Escreve dados em chunks (simula chegada de dados no tempo)
        n_chunks = 10
        chunk_size = max(1, len(df) // n_chunks)
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            chunk.to_parquet(f"{tmpdir}/chunk_{i:08d}.parquet", index=False)

        # Lê schema a partir dos arquivos escritos
        schema = spark.read.parquet(tmpdir).schema

        # Colunas disponíveis para agregação
        available = [c for c in SENSOR_COLS if c in df.columns]
        agg_exprs = [F.avg(c).alias(f"avg_{c.lower().replace('-', '_')}") for c in available]

        # Configura streaming query
        sdf = (
            spark.readStream
            .format("parquet")
            .schema(schema)
            .load(tmpdir)
        )

        result_sdf = (
            sdf
            .withColumn("event_time", (F.col("timestamp_ms") / 1000).cast("timestamp"))
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
            .withColumn(
                "processing_ts_ms",
                (F.unix_timestamp() * 1000).cast("long"),
            )
            .withColumn(
                "latency_ms",
                F.col("processing_ts_ms") - F.col("max_producer_ts_ms"),
            )
        )

        t_start = time.time()

        query = (
            result_sdf.writeStream
            .format("memory")
            .queryName("spark_results")
            .trigger(once=True)
            .start()
        )
        query.awaitTermination(timeout=600)

        t_end = time.time()
        total_time_s = t_end - t_start

        # Métricas do lastProgress
        progress = query.lastProgress or {}
        num_input_rows = progress.get("numInputRows", len(df))
        duration_ms = progress.get("durationMs", {})

        # Coleta latências das janelas processadas
        results_df = spark.sql(
            "SELECT latency_ms, record_count FROM spark_results WHERE latency_ms IS NOT NULL"
        )
        rows = results_df.collect()
        latencies = [r["latency_ms"] for r in rows if r["latency_ms"] is not None]
        window_sizes = [r["record_count"] for r in rows if r["record_count"] is not None]

        throughput = num_input_rows / total_time_s if total_time_s > 0 else 0.0

        metrics: dict[str, Any] = {
            "engine": "spark",
            "total_time_s": round(total_time_s, 3),
            "total_records": int(len(df)),
            "windows_processed": len(latencies),
            "throughput_rps": round(throughput, 2),
            "input_rows_per_second": progress.get("inputRowsPerSecond", throughput),
            "processed_rows_per_second": progress.get("processedRowsPerSecond", throughput),
            "trigger_execution_ms": duration_ms.get("triggerExecution", 0),
            "get_offset_ms": duration_ms.get("getOffset", 0),
            "latency_ms_raw": latencies,
            "window_sizes": window_sizes,
            "latency_p50": float(np.percentile(latencies, 50)) if latencies else 0.0,
            "latency_p95": float(np.percentile(latencies, 95)) if latencies else 0.0,
            "latency_p99": float(np.percentile(latencies, 99)) if latencies else 0.0,
            "latency_mean": float(np.mean(latencies)) if latencies else 0.0,
        }

        print(f"\n[Spark] Tempo total: {total_time_s:.2f}s | "
              f"Throughput: {throughput:.0f} rec/s | "
              f"Janelas: {len(latencies)} | "
              f"Latência p50/p95/p99: "
              f"{metrics['latency_p50']:.0f}/{metrics['latency_p95']:.0f}/{metrics['latency_p99']:.0f} ms")

        spark.stop()
        return metrics

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Benchmark Flink
# ---------------------------------------------------------------------------

def run_flink_benchmark(df: pd.DataFrame, output_dir: Path) -> dict[str, Any]:
    """
    Processa o DataFrame com PyFlink DataStream API (local mode).

    Estratégia:
      1. Converte o DataFrame em uma coleção Python.
      2. env.from_collection() cria um DataStream finito.
      3. Um MapFunction registra o timestamp de processamento por registro.
      4. execute_and_collect() executa o job e retorna os resultados.
      5. Calcula throughput e latência a partir dos timestamps.
    """
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.common.typeinfo import Types
        from pyflink.datastream.functions import MapFunction
    except ImportError:
        print("[ERRO] PyFlink não encontrado. Instale: pip install apache-flink")
        return {}

    available = [c for c in SENSOR_COLS if c in df.columns]

    # Define tipo de entrada: (ingest_ts_ms, well_id, timestamp_ms, event_code, *sensors)
    field_types = [
        Types.LONG(),    # ingest_ts_ms
        Types.STRING(),  # well_id
        Types.LONG(),    # timestamp_ms
        Types.INT(),     # event_code
    ] + [Types.DOUBLE()] * len(available)
    row_type = Types.TUPLE(field_types)

    # Tipo de saída: (well_id, timestamp_ms, latency_ms, proc_ts_ms)
    output_type = Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG(), Types.LONG()])

    class LatencyMapper(MapFunction):
        """Registra o timestamp de processamento e calcula latência."""
        def map(self, value):  # noqa: D102
            proc_ts = int(time.time() * 1000)
            ingest_ts = value[0]
            latency = proc_ts - ingest_ts
            return (value[1], value[2], latency, proc_ts)

    # Inicializa o ambiente Flink (aqui ocorre o startup da JVM)
    print("[Flink] Inicializando ambiente...")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Define ingestion timestamp APÓS o startup da JVM
    ingest_ts_ms = int(time.time() * 1000)

    print(f"[Flink] Preparando {len(df):,} registros...")

    # Pré-processa colunas para evitar overhead no loop
    well_ids = df["well_id"].astype(str).tolist()
    timestamps = df["timestamp_ms"].fillna(0).astype(int).tolist()
    event_codes = df["event_code"].fillna(0).astype(int).tolist()
    sensor_arrays = {c: df[c].fillna(0.0).astype(float).tolist() for c in available}

    records = []
    for i in range(len(df)):
        sensor_values = [sensor_arrays[c][i] for c in available]
        record = (ingest_ts_ms, well_ids[i], timestamps[i], event_codes[i], *sensor_values)
        records.append(record)

    # Cria DataStream a partir da coleção
    ds = env.from_collection(records, type_info=row_type)
    result_ds = ds.map(LatencyMapper(), output_type=output_type)

    print("[Flink] Executando job...")
    t_start = time.time()

    # execute_and_collect() roda o job e retorna todos os resultados
    collected = list(result_ds.execute_and_collect())

    t_end = time.time()
    total_time_s = t_end - t_start

    latencies = [int(item[2]) for item in collected]
    throughput = len(collected) / total_time_s if total_time_s > 0 else 0.0

    metrics: dict[str, Any] = {
        "engine": "flink",
        "total_time_s": round(total_time_s, 3),
        "total_records": len(records),
        "records_collected": len(collected),
        "throughput_rps": round(throughput, 2),
        "latency_ms_raw": latencies,
        "latency_p50": float(np.percentile(latencies, 50)) if latencies else 0.0,
        "latency_p95": float(np.percentile(latencies, 95)) if latencies else 0.0,
        "latency_p99": float(np.percentile(latencies, 99)) if latencies else 0.0,
        "latency_mean": float(np.mean(latencies)) if latencies else 0.0,
    }

    print(f"\n[Flink] Tempo total: {total_time_s:.2f}s | "
          f"Throughput: {throughput:.0f} rec/s | "
          f"Latência p50/p95/p99: "
          f"{metrics['latency_p50']:.0f}/{metrics['latency_p95']:.0f}/{metrics['latency_p99']:.0f} ms")

    return metrics


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark Spark vs Flink no dataset 3W (sem Kafka/Docker)"
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET_PATH,
        help=f"Caminho para o diretório raiz do dataset 3W (default: {DEFAULT_DATASET_PATH})",
    )
    parser.add_argument(
        "--engine",
        choices=["spark", "flink", "both"],
        default="both",
        help="Engine a executar (default: both)",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=5,
        help="Máximo de arquivos parquet por classe de evento (default: 5)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=100_000,
        help="Máximo de linhas a processar (0 = sem limite; default: 100000)",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Diretório para salvar métricas JSON (default: {DEFAULT_OUTPUT_DIR})",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. Carrega dataset
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"Carregando dataset 3W de: {args.dataset}")
    print(f"Max arquivos por classe: {args.max_files}")
    print(f"Max linhas: {'sem limite' if args.max_rows == 0 else f'{args.max_rows:,}'}")
    print(f"{'='*60}\n")

    df = load_3w_dataset(args.dataset, max_files_per_class=args.max_files, max_rows=args.max_rows)

    results: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # 2. Spark
    # ------------------------------------------------------------------
    if args.engine in ("spark", "both"):
        print(f"\n{'='*60}")
        print("Iniciando benchmark SPARK...")
        print(f"{'='*60}")
        spark_metrics = run_spark_benchmark(df, output_dir)
        if spark_metrics:
            results["spark"] = spark_metrics
            out_file = output_dir / "spark_metrics.json"
            with open(out_file, "w") as f:
                # Salva sem o array bruto (pode ser muito grande)
                to_save = {k: v for k, v in spark_metrics.items() if k != "latency_ms_raw"}
                to_save["latency_ms_sample"] = spark_metrics.get("latency_ms_raw", [])[:1000]
                json.dump(to_save, f, indent=2)
            print(f"  Métricas Spark salvas em: {out_file}")

    # ------------------------------------------------------------------
    # 3. Flink
    # ------------------------------------------------------------------
    if args.engine in ("flink", "both"):
        print(f"\n{'='*60}")
        print("Iniciando benchmark FLINK...")
        print(f"{'='*60}")
        flink_metrics = run_flink_benchmark(df, output_dir)
        if flink_metrics:
            results["flink"] = flink_metrics
            out_file = output_dir / "flink_metrics.json"
            with open(out_file, "w") as f:
                to_save = {k: v for k, v in flink_metrics.items() if k != "latency_ms_raw"}
                to_save["latency_ms_sample"] = flink_metrics.get("latency_ms_raw", [])[:1000]
                json.dump(to_save, f, indent=2)
            print(f"  Métricas Flink salvas em: {out_file}")

    # ------------------------------------------------------------------
    # 4. Resumo comparativo
    # ------------------------------------------------------------------
    if len(results) >= 1:
        print(f"\n{'='*60}")
        print("RESUMO")
        print(f"{'='*60}")
        header = f"{'Métrica':<30} {'Spark':>12} {'Flink':>12}"
        print(header)
        print("-" * len(header))

        def fmt(val: Any) -> str:
            if isinstance(val, float):
                return f"{val:>12.2f}"
            if isinstance(val, int):
                return f"{val:>12,}"
            return f"{str(val):>12}"

        metrics_to_show = [
            ("total_time_s", "Tempo total (s)"),
            ("total_records", "Total registros"),
            ("throughput_rps", "Throughput (rec/s)"),
            ("latency_p50", "Latência p50 (ms)"),
            ("latency_p95", "Latência p95 (ms)"),
            ("latency_p99", "Latência p99 (ms)"),
            ("latency_mean", "Latência média (ms)"),
        ]
        for key, label in metrics_to_show:
            spark_val = results.get("spark", {}).get(key, "N/A")
            flink_val = results.get("flink", {}).get(key, "N/A")
            print(f"{label:<30}{fmt(spark_val)}{fmt(flink_val)}")

        # Salva resumo consolidado
        summary_file = output_dir / "summary.json"
        with open(summary_file, "w") as f:
            summary = {}
            for engine, m in results.items():
                summary[engine] = {k: v for k, v in m.items() if k != "latency_ms_raw"}
                summary[engine]["latency_ms_sample"] = m.get("latency_ms_raw", [])[:1000]
            json.dump(summary, f, indent=2)
        print(f"\nResumo salvo em: {summary_file}")
        print(f"\nPara plotar os resultados:\n  python benchmark_simple/plot.py --results {output_dir}")


if __name__ == "__main__":
    main()
