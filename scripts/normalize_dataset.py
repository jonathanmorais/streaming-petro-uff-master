#!/usr/bin/env python3
"""
Normaliza o dataset 3W no S3 in-place:
  - TIMESTAMP(NANOS) → int64
  - class INT32      → float64
  - Brotli codec     → Snappy

Lê de SRC_BUCKET/SRC_PREFIX, grava em DST_BUCKET/DST_PREFIX.
Por padrão SRC == DST (overwrite in-place).

Variáveis de ambiente:
  SRC_PATH   s3://streaming-3w/3w/dataset   (origem)
  DST_PATH   s3://streaming-3w/3w/dataset   (destino — igual = overwrite)
  WORKERS    número de threads paralelas (default: 8)
"""

import io
import os
import sys
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def parse_s3_path(path: str):
    path = path.replace("s3a://", "").replace("s3://", "")
    bucket = path.split("/")[0]
    prefix = "/".join(path.split("/")[1:])
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


def normalize_table(table: pa.Table) -> pa.Table:
    cols = {}
    changed = False
    for field in table.schema:
        col = table.column(field.name)
        if field.name == "timestamp" and pa.types.is_timestamp(col.type):
            cols[field.name] = col.cast(pa.int64())
            changed = True
        elif field.name == "class" and col.type != pa.float64():
            cols[field.name] = col.cast(pa.float64())
            changed = True
        else:
            cols[field.name] = col
    if not changed:
        return None  # arquivo já está normalizado — pular
    return pa.table(cols, schema=pa.schema([
        pa.field(name, cols[name].type) for name in cols
    ]))


def process_file(s3, src_bucket, src_key, dst_bucket, dst_key) -> str:
    try:
        obj = s3.get_object(Bucket=src_bucket, Key=src_key)
        buf = io.BytesIO(obj["Body"].read())
        table = pq.read_table(buf)

        normalized = normalize_table(table)
        if normalized is None:
            return f"SKIP  {src_key} (já normalizado)"

        out = io.BytesIO()
        pq.write_table(normalized, out, compression="snappy")
        out.seek(0)

        s3.put_object(Bucket=dst_bucket, Key=dst_key, Body=out.read())
        return f"OK    {src_key}"

    except Exception as e:
        return f"ERROR {src_key}: {e}\n{traceback.format_exc()}"


def main():
    src_path = os.environ.get("SRC_PATH", "s3://streaming-3w/3w/dataset")
    dst_path = os.environ.get("DST_PATH", src_path)
    workers  = int(os.environ.get("WORKERS", "8"))

    src_bucket, src_prefix = parse_s3_path(src_path)
    dst_bucket, dst_prefix = parse_s3_path(dst_path)

    log.info(f"Origem : s3://{src_bucket}/{src_prefix}")
    log.info(f"Destino: s3://{dst_bucket}/{dst_prefix}")
    log.info(f"Workers: {workers}")

    s3 = boto3.client("s3")

    # Lista todos os arquivos parquet
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=src_bucket, Prefix=src_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    total = len(keys)
    log.info(f"Arquivos encontrados: {total}")
    if total == 0:
        log.error("Nenhum arquivo parquet encontrado. Verifique SRC_PATH.")
        sys.exit(1)

    ok = skip = errors = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for src_key in keys:
            # Substitui o prefix de origem pelo de destino
            relative = src_key[len(src_prefix):]
            dst_key = dst_prefix + relative
            futures[pool.submit(process_file, s3, src_bucket, src_key, dst_bucket, dst_key)] = src_key

        done = 0
        for future in as_completed(futures):
            result = future.result()
            done += 1
            if result.startswith("OK"):
                ok += 1
            elif result.startswith("SKIP"):
                skip += 1
            else:
                errors += 1
                log.warning(result)

            if done % 50 == 0 or done == total:
                log.info(f"Progresso: {done}/{total}  ok={ok} skip={skip} err={errors}")

    log.info("=" * 60)
    log.info(f"Concluído: {total} arquivos")
    log.info(f"  Normalizados : {ok}")
    log.info(f"  Já ok (skip) : {skip}")
    log.info(f"  Erros        : {errors}")

    if errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
