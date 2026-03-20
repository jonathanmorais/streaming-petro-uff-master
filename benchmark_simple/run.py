#!/usr/bin/env python3
"""
Benchmark simplificado: Spark vs Flink no dataset 3W (sem Kafka/Docker).

Suporte a GPU via:
  - cuDF  → leitura parquet e manipulação de DataFrame na GPU
  - CuPy  → agregação de janelas nos operadores Flink
  - RAPIDS Accelerator → SQL/shuffle do Spark na GPU (requer JAR)

Uso:
  python benchmark_simple/run.py                          # auto-detecta GPU
  python benchmark_simple/run.py --gpu                   # força GPU
  python benchmark_simple/run.py --cpu                   # força CPU
  python benchmark_simple/run.py --engine spark --max-rows 200000
  python benchmark_simple/run.py --engine flink --gpu --max-rows 500000
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Colunas de sensores disponíveis no dataset 3W v2
# ---------------------------------------------------------------------------
SENSOR_COLS = [
    "P-PDG", "P-TPT", "T-TPT", "P-MON-CKP", "T-JUS-CKP",
    "P-JUS-CKP", "P-MON-CKGL", "P-JUS-CKGL", "QGL", "QBS",
]

DEFAULT_DATASET_PATH = os.environ.get("DATASET_PATH", "/home/jonathan/3W/dataset")
DEFAULT_OUTPUT_DIR = Path(__file__).parent / "results"

# JAR do RAPIDS Accelerator para Spark (opcional — ver requirements.txt)
# Pode ser sobrescrito pela variável de ambiente RAPIDS_JAR_PATH
RAPIDS_JAR_PATH = os.environ.get(
    "RAPIDS_JAR_PATH",
    str(Path(__file__).parent / "rapids-4-spark.jar"),
)
# Maven coordinate para Spark 3.5 (Scala 2.12) — RAPIDS 24.x
RAPIDS_MAVEN_SPARK35 = "com.nvidia:rapids-4-spark_2.12:24.10.0"
# Maven coordinate para Spark 4.x (Scala 2.13) — RAPIDS 25.x
RAPIDS_MAVEN_SPARK4 = "com.nvidia:rapids-4-spark_2.13:25.02.0"

# ---------------------------------------------------------------------------
# Detecção de GPU
# ---------------------------------------------------------------------------

def detect_gpu() -> bool:
    """Retorna True se uma GPU NVIDIA estiver disponível via nvidia-smi."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            gpus = result.stdout.strip().splitlines()
            print(f"[GPU] Detectada: {', '.join(gpus)}")
            return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return False


def check_cudf() -> bool:
    """Retorna True se cuDF estiver instalado e funcional."""
    try:
        import cudf  # noqa: F401
        return True
    except ImportError:
        return False


def check_cupy() -> bool:
    """Retorna True se CuPy estiver instalado e funcional."""
    try:
        import cupy as cp
        cp.array([1.0])  # teste simples de alocação
        return True
    except (ImportError, Exception):
        return False


# ---------------------------------------------------------------------------
# Carregamento do dataset
# ---------------------------------------------------------------------------

def load_3w_dataset(
    dataset_path: str,
    max_files_per_class: int = 5,
    max_rows: int = 100_000,
    use_gpu: bool = False,
) -> pd.DataFrame:
    """
    Varre os diretórios 0-9 do dataset 3W, lê até max_files_per_class
    arquivos parquet por classe de evento e retorna um DataFrame pandas.

    Se use_gpu=True e cuDF estiver disponível, usa GPU para ler os parquets
    (até ~10× mais rápido) antes de converter para pandas.
    """
    dataset_path = Path(dataset_path)
    dfs: list[pd.DataFrame] = []
    total_files = 0

    use_cudf = use_gpu and check_cudf()
    if use_cudf:
        import cudf
        print("[GPU] cuDF disponível — lendo parquets na GPU")
    else:
        if use_gpu:
            print("[CPU] cuDF não encontrado — lendo com pandas (pip install cudf-cu12)")

    for event_dir in sorted(dataset_path.iterdir()):
        if not event_dir.is_dir() or not event_dir.name.isdigit():
            continue
        event_code = int(event_dir.name)

        parquet_files = sorted(event_dir.glob("*.parquet"))[:max_files_per_class]
        for pf in parquet_files:
            try:
                if use_cudf:
                    gdf = cudf.read_parquet(str(pf))
                    gdf.index.name = "timestamp"
                    gdf = gdf.reset_index()
                    df = gdf.to_pandas()
                else:
                    df = pd.read_parquet(pf)
                    df.index.name = "timestamp"
                    df = df.reset_index()

                stem = pf.stem
                source = "REAL" if stem.startswith("WELL-") else (
                    "SIMULATED" if stem.startswith("SIMULATED") else "DRAWN"
                )
                df["well_id"] = stem
                df["source"] = source
                df["event_code"] = event_code
                df["timestamp_ms"] = (
                    pd.to_datetime(df["timestamp"]).astype("int64") // 1_000_000
                )
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
        combined = (
            combined.sample(n=max_rows, random_state=42)
            .sort_values("timestamp_ms")
            .reset_index(drop=True)
        )
        print(f"Dataset: {total_files} arquivos | {len(combined):,} linhas "
              f"(amostrado de {total_loaded:,})")
    else:
        print(f"Dataset: {total_files} arquivos | {len(combined):,} linhas")

    return combined


# ---------------------------------------------------------------------------
# Benchmark Spark
# ---------------------------------------------------------------------------

def run_spark_benchmark(df: pd.DataFrame, output_dir: Path, use_gpu: bool = False) -> dict[str, Any]:
    """
    Processa o DataFrame com Spark Structured Streaming (trigger once).

    GPU via RAPIDS Accelerator para Spark:
      - Se o JAR estiver em RAPIDS_JAR_PATH ou for baixado via Maven,
        o Spark redireciona automaticamente SQL/shuffle/join para a GPU.
      - Sem o JAR, roda em CPU normalmente.

    Métricas coletadas:
      - throughput_rps: registros processados por segundo
      - latency_ms (por janela de 60s): processing_ts - ingestion_ts
      - batch duration, trigger execution time
    """
    try:
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as F
    except ImportError:
        print("[ERRO] PySpark não encontrado.")
        return {}

    tmpdir = tempfile.mkdtemp(prefix="3w_spark_")
    try:
        # ------------------------------------------------------------------
        # Configuração Spark
        # ------------------------------------------------------------------
        builder = (
            SparkSession.builder
            .appName("3W-Benchmark-Spark-GPU" if use_gpu else "3W-Benchmark-Spark")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "2g")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.streaming.checkpointLocation", f"{tmpdir}/checkpoint")
        )

        if use_gpu:
            rapids_jar = Path(RAPIDS_JAR_PATH)
            if rapids_jar.exists():
                print(f"[GPU] RAPIDS JAR encontrado: {rapids_jar}")
                builder = (
                    builder
                    .config("spark.jars", str(rapids_jar))
                    .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
                    .config("spark.rapids.sql.enabled", "true")
                    .config("spark.rapids.sql.incompatibleOps.enabled", "true")
                    .config("spark.rapids.memory.pinnedPool.size", "2g")
                    .config("spark.executor.resource.gpu.amount", "1")
                    .config("spark.task.resource.gpu.amount", "0.25")
                    .config("spark.rapids.sql.concurrentGpuTasks", "4")
                )
                print("[GPU] RAPIDS Accelerator ativado para Spark SQL")
            else:
                # Tenta baixar o JAR via Maven (requer conexão com internet)
                try:
                    import pyspark
                    spark_ver = pyspark.__version__
                    maven_coord = (
                        RAPIDS_MAVEN_SPARK4 if spark_ver.startswith("4")
                        else RAPIDS_MAVEN_SPARK35
                    )
                    print(f"[GPU] JAR RAPIDS não encontrado em {RAPIDS_JAR_PATH}")
                    print(f"[GPU] Tentando baixar via Maven: {maven_coord}")
                    builder = (
                        builder
                        .config("spark.jars.packages", maven_coord)
                        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
                        .config("spark.rapids.sql.enabled", "true")
                        .config("spark.rapids.sql.incompatibleOps.enabled", "true")
                        .config("spark.executor.resource.gpu.amount", "1")
                        .config("spark.task.resource.gpu.amount", "0.25")
                    )
                except Exception as exc:
                    print(f"[WARN] Não foi possível configurar RAPIDS: {exc}")
                    print("[WARN] Spark rodará em modo CPU")

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        # ------------------------------------------------------------------
        # Prepara fonte de streaming (parquet chunks)
        # ------------------------------------------------------------------
        ingestion_ts_ms = int(time.time() * 1000)
        df_copy = df.copy()
        df_copy["producer_ts_ms"] = ingestion_ts_ms

        n_chunks = min(20, max(1, len(df_copy) // 5000))
        chunk_size = max(1, len(df_copy) // n_chunks)
        for i in range(0, len(df_copy), chunk_size):
            df_copy.iloc[i : i + chunk_size].to_parquet(
                f"{tmpdir}/chunk_{i:08d}.parquet", index=False
            )

        schema = spark.read.parquet(tmpdir).schema

        # ------------------------------------------------------------------
        # Pipeline de processamento com janela de 60s
        # ------------------------------------------------------------------
        available = [c for c in SENSOR_COLS if c in df.columns]
        agg_exprs = [
            F.avg(c).alias(f"avg_{c.lower().replace('-', '_')}")
            for c in available
        ] + [
            F.stddev(c).alias(f"std_{c.lower().replace('-', '_')}")
            for c in available
        ]

        sdf = spark.readStream.format("parquet").schema(schema).load(tmpdir)

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

        progress = query.lastProgress or {}
        num_input_rows = progress.get("numInputRows", len(df))
        duration_ms = progress.get("durationMs", {})

        results_rows = spark.sql(
            "SELECT latency_ms, record_count FROM spark_results WHERE latency_ms IS NOT NULL"
        ).collect()
        latencies = [r["latency_ms"] for r in results_rows if r["latency_ms"] is not None]
        window_sizes = [r["record_count"] for r in results_rows if r["record_count"] is not None]

        throughput = num_input_rows / total_time_s if total_time_s > 0 else 0.0

        metrics: dict[str, Any] = {
            "engine": "spark",
            "gpu_enabled": use_gpu,
            "total_time_s": round(total_time_s, 3),
            "total_records": int(len(df)),
            "windows_processed": len(latencies),
            "throughput_rps": round(throughput, 2),
            "input_rows_per_second": progress.get("inputRowsPerSecond", throughput),
            "processed_rows_per_second": progress.get("processedRowsPerSecond", throughput),
            "trigger_execution_ms": duration_ms.get("triggerExecution", 0),
            "latency_ms_raw": latencies,
            "window_sizes": window_sizes,
            "latency_p50": float(np.percentile(latencies, 50)) if latencies else 0.0,
            "latency_p95": float(np.percentile(latencies, 95)) if latencies else 0.0,
            "latency_p99": float(np.percentile(latencies, 99)) if latencies else 0.0,
            "latency_mean": float(np.mean(latencies)) if latencies else 0.0,
        }

        tag = "GPU" if use_gpu else "CPU"
        print(f"\n[Spark/{tag}] Tempo: {total_time_s:.2f}s | "
              f"Throughput: {throughput:.0f} rec/s | "
              f"Janelas: {len(latencies)} | "
              f"Latência p50/p95/p99: "
              f"{metrics['latency_p50']:.0f}/"
              f"{metrics['latency_p95']:.0f}/"
              f"{metrics['latency_p99']:.0f} ms")

        spark.stop()
        return metrics

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Benchmark Flink
# ---------------------------------------------------------------------------

class _GPUWindowAggregator:
    """
    Acumulador de janela por well_id que usa CuPy (GPU) ou NumPy (CPU)
    para calcular média e desvio padrão de cada sensor.

    Implementa uma janela de contagem: emite resultado a cada `window_size`
    registros acumulados por well, o que simula uma janela temporal no contexto
    de dados históricos com taxa de 1 Hz.
    """

    def __init__(self, n_sensors: int, window_size: int = 60, use_gpu: bool = False):
        self._n = n_sensors
        self._window_size = window_size
        self._use_gpu = use_gpu and check_cupy()
        self._buffers: dict[str, dict] = {}
        if self._use_gpu:
            import cupy as cp
            self._xp = cp
        else:
            self._xp = np

    def push(
        self,
        ingest_ts: int,
        well_id: str,
        ts_ms: int,
        event_code: int,
        sensors: list[float],
    ) -> tuple | None:
        """
        Adiciona um registro ao buffer do well. Retorna resultado da janela
        (ou None se a janela ainda não atingiu o tamanho mínimo).
        """
        buf = self._buffers.setdefault(
            well_id, {"data": [], "first_ingest": ingest_ts}
        )
        buf["data"].append(sensors)

        if len(buf["data"]) >= self._window_size:
            xp = self._xp
            try:
                arr = xp.array(buf["data"], dtype=xp.float32)
                # nanmean/nanstd não existe no CuPy puro — usa where para mascarar NaN
                if self._use_gpu:
                    import cupy as cp
                    valid = cp.where(cp.isnan(arr), 0.0, arr)
                    count = cp.sum(~cp.isnan(arr), axis=0).clip(1)
                    means = (cp.sum(valid, axis=0) / count).tolist()
                    diff = valid - cp.sum(valid, axis=0, keepdims=True) / count
                    stds = cp.sqrt(cp.sum(diff ** 2, axis=0) / count).tolist()
                else:
                    means = np.nanmean(arr, axis=0).tolist()
                    stds = np.nanstd(arr, axis=0).tolist()
            except Exception:
                # Fallback para NumPy em caso de erro GPU
                arr_np = np.array(buf["data"], dtype=np.float32)
                means = np.nanmean(arr_np, axis=0).tolist()
                stds = np.nanstd(arr_np, axis=0).tolist()

            first_ingest = buf["first_ingest"]
            self._buffers[well_id] = {
                "data": [],
                "first_ingest": int(time.time() * 1000),
            }

            proc_ts = int(time.time() * 1000)
            latency = proc_ts - first_ingest
            return (well_id, ts_ms, event_code, latency, proc_ts, *means, *stds)

        return None

    def flush_all(self) -> list[tuple]:
        """Emite janelas parciais (final de stream)."""
        results = []
        for well_id, buf in self._buffers.items():
            if not buf["data"]:
                continue
            try:
                xp = self._xp
                arr = xp.array(buf["data"], dtype=xp.float32)
                if self._use_gpu:
                    import cupy as cp
                    valid = cp.where(cp.isnan(arr), 0.0, arr)
                    count = cp.sum(~cp.isnan(arr), axis=0).clip(1)
                    means = (cp.sum(valid, axis=0) / count).tolist()
                    diff = valid - cp.sum(valid, axis=0, keepdims=True) / count
                    stds = cp.sqrt(cp.sum(diff ** 2, axis=0) / count).tolist()
                else:
                    means = np.nanmean(arr, axis=0).tolist()
                    stds = np.nanstd(arr, axis=0).tolist()
            except Exception:
                arr_np = np.array(buf["data"], dtype=np.float32)
                means = np.nanmean(arr_np, axis=0).tolist()
                stds = np.nanstd(arr_np, axis=0).tolist()

            proc_ts = int(time.time() * 1000)
            latency = proc_ts - buf["first_ingest"]
            results.append((well_id, 0, 0, latency, proc_ts, *means, *stds))
        return results


def run_flink_benchmark(
    df: pd.DataFrame, output_dir: Path, use_gpu: bool = False
) -> dict[str, Any]:
    """
    Processa o DataFrame com PyFlink DataStream API (local mode).

    GPU via CuPy:
      - A classe _GPUWindowAggregator usa CuPy para calcular mean/std
        de cada janela de 60 registros por well (equivalente à janela
        de 60s do pipeline principal).
      - Fallback automático para NumPy se CuPy não estiver disponível.

    Métricas coletadas:
      - throughput_rps: total_records / total_execution_time
      - latency_ms por janela: processing_ts - first_ingest_ts_da_janela
    """
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.common.typeinfo import Types
        from pyflink.datastream.functions import MapFunction
    except ImportError:
        print("[ERRO] PyFlink não encontrado. Instale: pip install apache-flink")
        return {}

    use_cupy = use_gpu and check_cupy()
    tag = "GPU/CuPy" if use_cupy else "CPU/NumPy"
    if use_gpu and not use_cupy:
        print("[WARN] CuPy não encontrado — Flink rodará em CPU (pip install cupy-cuda12x)")

    available = [c for c in SENSOR_COLS if c in df.columns]
    n_sensors = len(available)

    # Tipos PyFlink para o DataStream de entrada
    field_types = [
        Types.LONG(),    # ingest_ts_ms
        Types.STRING(),  # well_id
        Types.LONG(),    # timestamp_ms
        Types.INT(),     # event_code
    ] + [Types.DOUBLE()] * n_sensors
    row_type = Types.TUPLE(field_types)

    # Tipo de saída: (well_id, ts_ms, event_code, latency_ms, proc_ts_ms, *means, *stds)
    n_stats = n_sensors * 2
    output_type = Types.TUPLE(
        [Types.STRING(), Types.LONG(), Types.INT(), Types.LONG(), Types.LONG()]
        + [Types.DOUBLE()] * n_stats
    )

    class WindowAggMapFunction(MapFunction):
        """
        Wrapper PyFlink em torno de _GPUWindowAggregator.
        Instanciado uma vez por task (parallelism=1 garante estado compartilhado).
        """
        def __init__(self, n_sens: int, gpu: bool):
            self._n = n_sens
            self._gpu = gpu
            self._agg: _GPUWindowAggregator | None = None

        def open(self, runtime_context):  # noqa: D102
            self._agg = _GPUWindowAggregator(
                n_sensors=self._n, window_size=60, use_gpu=self._gpu
            )

        def map(self, value):  # noqa: D102
            ingest_ts = value[0]
            well_id = value[1]
            ts_ms = value[2]
            event_code = value[3]
            sensors = [value[4 + i] for i in range(self._n)]
            result = self._agg.push(ingest_ts, well_id, ts_ms, event_code, sensors)
            if result is not None:
                return result
            # Registro ainda em buffer — emite placeholder para manter o stream fluindo
            proc_ts = int(time.time() * 1000)
            return (well_id, ts_ms, event_code, proc_ts - ingest_ts, proc_ts,
                    *[0.0] * n_stats)

    # ------------------------------------------------------------------
    # Inicializa ambiente PyFlink (startup JVM aqui)
    # ------------------------------------------------------------------
    print("[Flink] Inicializando ambiente PyFlink...")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # parallelism=1 para estado correto no WindowAgg

    # Marca o tempo de ingestão APÓS a JVM estar pronta
    ingest_ts_ms = int(time.time() * 1000)

    # ------------------------------------------------------------------
    # Converte DataFrame em lista de tuplas (vetorizado, evita iterrows)
    # ------------------------------------------------------------------
    print(f"[Flink] Preparando {len(df):,} registros ({tag})...")
    col_data = {c: df[c].fillna(0.0).astype(float).tolist() for c in available}
    well_ids = df["well_id"].astype(str).tolist()
    timestamps = df["timestamp_ms"].fillna(0).astype(int).tolist()
    event_codes = df["event_code"].fillna(0).astype(int).tolist()

    records = [
        (ingest_ts_ms, well_ids[i], timestamps[i], event_codes[i],
         *[col_data[c][i] for c in available])
        for i in range(len(df))
    ]

    # ------------------------------------------------------------------
    # Cria pipeline e executa
    # ------------------------------------------------------------------
    ds = env.from_collection(records, type_info=row_type)
    result_ds = ds.map(
        WindowAggMapFunction(n_sensors, use_cupy),
        output_type=output_type,
    )

    print(f"[Flink] Executando job ({tag})...")
    t_start = time.time()
    collected = list(result_ds.execute_and_collect())
    t_end = time.time()

    total_time_s = t_end - t_start
    throughput = len(df) / total_time_s if total_time_s > 0 else 0.0

    # Latências apenas de janelas completas (stds != 0 indica janela real)
    window_latencies = [
        int(item[3]) for item in collected
        if any(item[5 + n_sensors + k] != 0.0 for k in range(n_sensors))
    ]
    all_latencies = [int(item[3]) for item in collected]

    metrics: dict[str, Any] = {
        "engine": "flink",
        "gpu_enabled": use_gpu,
        "cupy_used": use_cupy,
        "total_time_s": round(total_time_s, 3),
        "total_records": len(records),
        "records_collected": len(collected),
        "windows_emitted": len(window_latencies),
        "throughput_rps": round(throughput, 2),
        "latency_ms_raw": window_latencies or all_latencies[:5000],
        "latency_p50": float(np.percentile(window_latencies, 50)) if window_latencies else 0.0,
        "latency_p95": float(np.percentile(window_latencies, 95)) if window_latencies else 0.0,
        "latency_p99": float(np.percentile(window_latencies, 99)) if window_latencies else 0.0,
        "latency_mean": float(np.mean(window_latencies)) if window_latencies else 0.0,
    }

    print(f"\n[Flink/{tag}] Tempo: {total_time_s:.2f}s | "
          f"Throughput: {throughput:.0f} rec/s | "
          f"Janelas: {len(window_latencies)} | "
          f"Latência p50/p95/p99: "
          f"{metrics['latency_p50']:.0f}/"
          f"{metrics['latency_p95']:.0f}/"
          f"{metrics['latency_p99']:.0f} ms")

    return metrics


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark Spark vs Flink no dataset 3W com suporte a GPU"
    )
    parser.add_argument(
        "--dataset", default=DEFAULT_DATASET_PATH,
        help=f"Diretório raiz do dataset 3W (default: {DEFAULT_DATASET_PATH})",
    )
    parser.add_argument(
        "--engine", choices=["spark", "flink", "both"], default="both",
        help="Engine a executar (default: both)",
    )
    parser.add_argument(
        "--max-files", type=int, default=5,
        help="Máximo de arquivos parquet por classe de evento (default: 5)",
    )
    parser.add_argument(
        "--max-rows", type=int, default=100_000,
        help="Máximo de linhas a processar (0 = sem limite; default: 100000)",
    )
    parser.add_argument(
        "--output-dir", default=str(DEFAULT_OUTPUT_DIR),
        help=f"Diretório para salvar métricas JSON (default: {DEFAULT_OUTPUT_DIR})",
    )

    # Flags GPU
    gpu_group = parser.add_mutually_exclusive_group()
    gpu_group.add_argument(
        "--gpu", action="store_true", default=None,
        help="Força uso de GPU (cuDF para dados, CuPy para Flink, RAPIDS para Spark)",
    )
    gpu_group.add_argument(
        "--cpu", action="store_false", dest="gpu",
        help="Força modo CPU (desativa GPU mesmo que disponível)",
    )

    args = parser.parse_args()

    # Auto-detecta GPU se não especificado
    if args.gpu is None:
        args.gpu = detect_gpu()
        if not args.gpu:
            print("[INFO] GPU não detectada — rodando em modo CPU")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  Benchmark 3W — {'GPU' if args.gpu else 'CPU'} mode")
    print(f"  Dataset : {args.dataset}")
    print(f"  Engine  : {args.engine}")
    print(f"  Max files/classe: {args.max_files}")
    print(f"  Max rows: {'sem limite' if args.max_rows == 0 else f'{args.max_rows:,}'}")
    print(f"{'='*60}\n")

    # ------------------------------------------------------------------
    # 1. Carrega dataset
    # ------------------------------------------------------------------
    df = load_3w_dataset(
        args.dataset,
        max_files_per_class=args.max_files,
        max_rows=args.max_rows,
        use_gpu=args.gpu,
    )

    results: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # 2. Spark
    # ------------------------------------------------------------------
    if args.engine in ("spark", "both"):
        print(f"\n{'='*60}")
        print(f"  Spark {'(GPU/RAPIDS)' if args.gpu else '(CPU)'}")
        print(f"{'='*60}")
        spark_metrics = run_spark_benchmark(df, output_dir, use_gpu=args.gpu)
        if spark_metrics:
            results["spark"] = spark_metrics
            _save_metrics(spark_metrics, output_dir / "spark_metrics.json")
            print(f"  → Métricas salvas: {output_dir / 'spark_metrics.json'}")

    # ------------------------------------------------------------------
    # 3. Flink
    # ------------------------------------------------------------------
    if args.engine in ("flink", "both"):
        print(f"\n{'='*60}")
        print(f"  Flink {'(GPU/CuPy)' if args.gpu else '(CPU/NumPy)'}")
        print(f"{'='*60}")
        flink_metrics = run_flink_benchmark(df, output_dir, use_gpu=args.gpu)
        if flink_metrics:
            results["flink"] = flink_metrics
            _save_metrics(flink_metrics, output_dir / "flink_metrics.json")
            print(f"  → Métricas salvas: {output_dir / 'flink_metrics.json'}")

    # ------------------------------------------------------------------
    # 4. Resumo
    # ------------------------------------------------------------------
    if results:
        _print_summary(results)
        summary_file = output_dir / "summary.json"
        summary = {}
        for engine, m in results.items():
            summary[engine] = {k: v for k, v in m.items() if k != "latency_ms_raw"}
            summary[engine]["latency_ms_sample"] = m.get("latency_ms_raw", [])[:2000]
        with open(summary_file, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"\nResumo salvo em: {summary_file}")
        print(f"Para plotar: python benchmark_simple/plot.py --results {output_dir}")


def _save_metrics(metrics: dict, path: Path) -> None:
    to_save = {k: v for k, v in metrics.items() if k != "latency_ms_raw"}
    to_save["latency_ms_sample"] = metrics.get("latency_ms_raw", [])[:2000]
    with open(path, "w") as f:
        json.dump(to_save, f, indent=2)


def _print_summary(results: dict) -> None:
    engines = list(results.keys())
    print(f"\n{'='*60}")
    print("  RESUMO COMPARATIVO")
    print(f"{'='*60}")
    col_w = 14
    header = f"  {'Métrica':<32}" + "".join(f"{e.capitalize():>{col_w}}" for e in engines)
    print(header)
    print("  " + "-" * (32 + col_w * len(engines)))
    for key, label in [
        ("gpu_enabled",    "GPU ativado"),
        ("total_time_s",   "Tempo total (s)"),
        ("total_records",  "Total registros"),
        ("throughput_rps", "Throughput (rec/s)"),
        ("latency_p50",    "Latência P50 (ms)"),
        ("latency_p95",    "Latência P95 (ms)"),
        ("latency_p99",    "Latência P99 (ms)"),
        ("latency_mean",   "Latência média (ms)"),
    ]:
        row = f"  {label:<32}"
        for e in engines:
            val = results[e].get(key, "N/A")
            if isinstance(val, bool):
                row += f"{'Sim' if val else 'Não':>{col_w}}"
            elif isinstance(val, float):
                row += f"{val:>{col_w}.2f}"
            elif isinstance(val, int):
                row += f"{val:>{col_w},}"
            else:
                row += f"{str(val):>{col_w}}"
        print(row)
    print()


if __name__ == "__main__":
    main()
