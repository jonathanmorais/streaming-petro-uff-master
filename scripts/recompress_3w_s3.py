"""
Re-compressão do dataset 3W: Brotli → Snappy
Para rodar no Google Colab (CPU ou GPU).

Fluxo:
  S3 (Brotli) → download em memória → re-comprime (Snappy) → S3 (Snappy)

Uso:
  1. Abra o Google Colab (qualquer runtime — GPU não é obrigatória)
  2. Faça upload deste script ou cole numa célula
  3. Preencha as variáveis BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
  4. Execute
"""

# =============================================================================
# CÉLULA 1 — Instalar dependências
# =============================================================================
# !pip install -q boto3 pyarrow

# =============================================================================
# CÉLULA 2 — Configuração
# =============================================================================
import os

# Credenciais AWS (use uma IAM key com s3:GetObject + s3:PutObject + s3:ListBucket)
os.environ["AWS_ACCESS_KEY_ID"]     = "SUA_ACCESS_KEY"
os.environ["AWS_SECRET_ACCESS_KEY"] = "SUA_SECRET_KEY"
os.environ["AWS_DEFAULT_REGION"]    = "us-east-1"

BUCKET      = "streaming-3w"
PREFIX      = "3w/dataset"          # prefixo no S3
COMPRESSION = "snappy"              # codec de destino (snappy ou zstd)
MAX_WORKERS = 4                     # downloads/uploads paralelos

# =============================================================================
# CÉLULA 3 — Re-compressão
# =============================================================================
import io
import time
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from concurrent.futures import ThreadPoolExecutor, as_completed

s3 = boto3.client("s3")

def list_parquet_files(bucket: str, prefix: str) -> list[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def recompress(bucket: str, key: str, compression: str) -> dict:
    """
    Baixa um parquet do S3, re-comprime e re-sobe no mesmo path.
    Retorna dict com status e tamanho original/novo.
    """
    result = {"key": key, "ok": False, "original_bytes": 0, "new_bytes": 0}
    try:
        # Download em memória
        resp = s3.get_object(Bucket=bucket, Key=key)
        data = resp["Body"].read()
        result["original_bytes"] = len(data)

        # Lê com pyarrow (suporta Brotli nativamente via libbrotli)
        buf_in = io.BytesIO(data)
        table = pq.read_table(buf_in)

        # Re-comprime
        buf_out = io.BytesIO()
        pq.write_table(table, buf_out, compression=compression)
        buf_out.seek(0)
        new_data = buf_out.read()
        result["new_bytes"] = len(new_data)

        # Upload (sobrescreve o mesmo key)
        s3.put_object(Bucket=bucket, Key=key, Body=new_data)
        result["ok"] = True

    except Exception as e:
        result["error"] = str(e)

    return result


def run(bucket: str, prefix: str, compression: str, max_workers: int):
    print(f"Listando arquivos em s3://{bucket}/{prefix} ...")
    keys = list_parquet_files(bucket, prefix)
    print(f"  {len(keys)} arquivos encontrados\n")

    t0 = time.time()
    ok = err = 0
    original_total = new_total = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(recompress, bucket, k, compression): k for k in keys}

        for i, future in enumerate(as_completed(futures), 1):
            r = future.result()
            if r["ok"]:
                ok += 1
                original_total += r["original_bytes"]
                new_total      += r["new_bytes"]
                status = "✓"
            else:
                err += 1
                status = f"✗ {r.get('error', '')[:60]}"

            if i % 50 == 0 or not r["ok"]:
                pct = i / len(keys) * 100
                print(f"  [{i:4d}/{len(keys)}] {pct:5.1f}%  {status}  {r['key'].split('/')[-1]}")

    elapsed = time.time() - t0
    ratio = new_total / original_total if original_total else 1

    print(f"\n{'='*55}")
    print(f"  Concluído em {elapsed:.0f}s")
    print(f"  Sucesso : {ok}   Erros: {err}")
    print(f"  Original: {original_total/1e6:.1f} MB")
    print(f"  Novo    : {new_total/1e6:.1f} MB  ({ratio:.2f}x)")
    print(f"{'='*55}")


run(BUCKET, PREFIX, COMPRESSION, MAX_WORKERS)

# =============================================================================
# CÉLULA 4 — (opcional) verificar um arquivo
# =============================================================================
# import boto3, io, pyarrow.parquet as pq
#
# s3   = boto3.client("s3")
# resp = s3.get_object(Bucket=BUCKET, Key="3w/dataset/8/WELL-00019_20120601165020.parquet")
# meta = pq.read_metadata(io.BytesIO(resp["Body"].read()))
# print(meta.row_group(0).column(0).compression)   # deve imprimir SNAPPY
