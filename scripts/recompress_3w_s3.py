"""
Re-compressão do dataset 3W: Brotli → Snappy + upload para S3
Para rodar no Google Colab (CPU ou GPU).

Fluxo:
  GitHub (git clone com LFS) → re-comprime localmente (Snappy) → S3 (Snappy)

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
# !sudo apt-get install -y git-lfs -q
# !git lfs install

# =============================================================================
# CÉLULA 2 — Clonar repositório 3W (com Git LFS para os parquets)
# =============================================================================
# !git clone https://github.com/petrobras/3W.git /content/3W
# !cd /content/3W && git lfs pull

# =============================================================================
# CÉLULA 3 — Configuração
# =============================================================================
import os

# Credenciais AWS (use uma IAM key com s3:PutObject + s3:ListBucket)
os.environ["AWS_ACCESS_KEY_ID"]     = "SUA_ACCESS_KEY"
os.environ["AWS_SECRET_ACCESS_KEY"] = "SUA_SECRET_KEY"
os.environ["AWS_DEFAULT_REGION"]    = "us-east-1"

DATASET_LOCAL = "/content/3W/dataset"  # caminho local após git clone
BUCKET        = "streaming-3w"
S3_PREFIX     = "3w/dataset"           # destino no S3
COMPRESSION   = "snappy"               # codec de destino
MAX_WORKERS   = 4                      # uploads paralelos

# =============================================================================
# CÉLULA 4 — Re-compressão + upload
# =============================================================================
import io
import time
import pathlib
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from concurrent.futures import ThreadPoolExecutor, as_completed

s3 = boto3.client("s3")


def collect_parquet_files(local_dir: str) -> list[pathlib.Path]:
    return sorted(pathlib.Path(local_dir).rglob("*.parquet"))


def recompress_and_upload(local_path: pathlib.Path, local_root: pathlib.Path,
                          bucket: str, s3_prefix: str, compression: str) -> dict:
    """
    Lê um parquet local (suporta Brotli via libbrotli do pyarrow),
    re-comprime para `compression` e faz upload para S3.
    """
    # Calcula o key S3 mantendo a estrutura de diretórios relativa
    rel = local_path.relative_to(local_root)
    s3_key = f"{s3_prefix}/{rel}".replace("\\", "/")

    result = {"path": str(local_path), "key": s3_key, "ok": False,
              "original_bytes": 0, "new_bytes": 0}
    try:
        result["original_bytes"] = local_path.stat().st_size

        # Lê com pyarrow (suporta Brotli nativamente via libbrotli)
        table = pq.read_table(str(local_path))

        # Re-comprime em memória
        buf_out = io.BytesIO()
        pq.write_table(table, buf_out, compression=compression)
        buf_out.seek(0)
        new_data = buf_out.read()
        result["new_bytes"] = len(new_data)

        # Upload para S3
        s3.put_object(Bucket=bucket, Key=s3_key, Body=new_data)
        result["ok"] = True

    except Exception as e:
        result["error"] = str(e)

    return result


def run(local_dir: str, bucket: str, s3_prefix: str,
        compression: str, max_workers: int):
    root = pathlib.Path(local_dir)
    files = collect_parquet_files(local_dir)
    total = len(files)
    print(f"Encontrados {total} arquivos em {local_dir}\n")

    t0 = time.time()
    ok = err = 0
    original_total = new_total = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(recompress_and_upload, f, root, bucket, s3_prefix, compression): f
            for f in files
        }

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
                pct = i / total * 100
                name = pathlib.Path(r["path"]).name
                print(f"  [{i:4d}/{total}] {pct:5.1f}%  {status}  {name}  →  {r['key']}")

    elapsed = time.time() - t0
    ratio = new_total / original_total if original_total else 1

    print(f"\n{'='*60}")
    print(f"  Concluído em {elapsed:.0f}s")
    print(f"  Sucesso : {ok}   Erros: {err}")
    print(f"  Original: {original_total/1e6:.1f} MB")
    print(f"  Novo    : {new_total/1e6:.1f} MB  ({ratio:.2f}x)")
    print(f"  S3      : s3://{bucket}/{s3_prefix}/")
    print(f"{'='*60}")


run(DATASET_LOCAL, BUCKET, S3_PREFIX, COMPRESSION, MAX_WORKERS)

# =============================================================================
# CÉLULA 5 — (opcional) verificar um arquivo no S3
# =============================================================================
# import boto3, io, pyarrow.parquet as pq
#
# s3   = boto3.client("s3")
# resp = s3.get_object(Bucket=BUCKET, Key="3w/dataset/8/WELL-00019_20120601165020.parquet")
# meta = pq.read_metadata(io.BytesIO(resp["Body"].read()))
# print(meta.row_group(0).column(0).compression)   # deve imprimir SNAPPY
