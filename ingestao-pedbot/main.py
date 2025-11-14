# import psycopg2
# from psycopg2 import OperationalError

# def create_connection():
#     try:
#         # Cria a conexão
#         connection = psycopg2.connect(
#             host="banco-bi-venancio.pedbot.com.br",        # Ex: "localhost" ou "10.0.0.5"
#             port="5432",                 # Porta padrão do PostgreSQL
#             database="pedbot",   # Nome do banco de dados
#             user="venancio",     # Usuário
#             password="RW3#b%YsG&Knxg2*LZ"    # Senha
#         )
#         print("✅ Conexão com o PostgreSQL estabelecida com sucesso!")
#         return connection

#     except OperationalError as e:
#         print(f"❌ Erro ao conectar ao PostgreSQL: {e}")
#         return None

# conn = create_connection()

# if conn:
#     # Cria um cursor para executar comandos SQL
#     cursor = conn.cursor()
#     cursor.execute("SELECT * FROM venancio_instances")
#     record = cursor.fetchone()
#     print(f"Versão do PostgreSQL: {record[0]}")

#     # Fecha a conexão
#     cursor.close()
#     conn.close()
#     print("🔒 Conexão encerrada.")



import os
import math
import sys
import time
import pytz
from datetime import datetime
from urllib.parse import quote_plus
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

import pandas as pd
from sqlalchemy import create_engine, text
from google.cloud import storage

load_dotenv()

# ========= Config (via env vars) =========
PG_HOST        = os.environ.get("HOST", "")
PG_PORT        = os.environ.get("PORT", "5432")
PG_DB          = os.environ.get("DB", "")
PG_USER        = os.environ.get("USER", "")
PG_PASSWORD    = os.environ.get("PASSWORD", "")
PG_SCHEMA      = os.environ.get("SCHEMA", "")
PG_TABLE       = os.environ.get("TABLE", "")

GCS_BUCKET     = os.environ.get("GCS_BUCKET", "")
BASE_PREFIX    = os.environ.get("BASE_PREFIX", "postgre")  # não inclua / no final
CHUNK_ROWS     = int(os.environ.get("CHUNK_ROWS", "100000"))        # ajuste conforme memória
DATE_OVERRIDE  = os.environ.get("DATE_OVERRIDE", "")                # opcional: "2025-11-12"
TZ             = os.environ.get("LOCAL_TZ", "America/Sao_Paulo")    # controla dt=...

# ========= Helpers =========
def log(msg):
    print(f"[{datetime.utcnow().isoformat()}Z] {msg}", flush=True)

def today_str():
    if DATE_OVERRIDE:
        return DATE_OVERRIDE
    tz = pytz.timezone(TZ)
    return datetime.now(tz).strftime("%Y-%m-%d")

def gcs_upload(local_path: str, bucket: str, obj_path: str):
    client = storage.Client()
    bucket_obj = client.bucket(bucket)
    blob = bucket_obj.blob(obj_path)
    blob.upload_from_filename(local_path)

def build_engine():
    # URL-encode da senha/usuário para evitar problema com #, @, $ etc.
    user = quote_plus(PG_USER)
    pwd  = quote_plus(PG_PASSWORD)
    host = PG_HOST
    port = PG_PORT
    db   = PG_DB
    # psycopg2 driver
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)

def qualify_table(table: str) -> str:
    # evita injeção simples; schema e tabela entre aspas duplas
    return f'"{table}"'

def count_rows(engine, schema, table):
    q = text(f"SELECT COUNT(*) FROM {qualify_table(table)}")
    with engine.connect() as conn:
        return conn.execute(q).scalar()

def export_full_table_to_gcs():
    # validações mínimas
    required = [PG_HOST, PG_DB, PG_USER, PG_PASSWORD, PG_SCHEMA, PG_TABLE, GCS_BUCKET]
    if any(not v for v in required):
        log("❌ Variáveis obrigatórias ausentes. Verifique PG_* e GCS_BUCKET.")
        sys.exit(2)

    dt_str = today_str()
    base_prefix = f"{BASE_PREFIX}/{PG_DB}/{PG_SCHEMA}/dt={dt_str}/{PG_TABLE}".replace("//", "/")

    engine = build_engine()
    total = count_rows(engine, PG_SCHEMA, PG_TABLE)
    log(f"Total de linhas em {PG_SCHEMA}.{PG_TABLE}: {total}")

    if total == 0:
        log("Tabela vazia. Nada para exportar.")
        return

    # Leitura em chunks via pandas.read_sql_query
    sql = f"SELECT * FROM {qualify_table(PG_TABLE)}"
    chunk_idx = 0
    rows_done = 0
    t0 = time.time()

    # /tmp tem armazenamento efêmero na Cloud Run; apague os arquivos após o upload
    for df in pd.read_sql_query(sql, engine, chunksize=CHUNK_ROWS):
        part_name = f"part-{chunk_idx:05d}.parquet"
        gcs_path = f"{base_prefix}/{part_name}"

        print(f"⬆️ Gravando chunk {chunk_idx} → {gcs_path}")
        gcs = fs.GcsFileSystem()
        table = pa.Table.from_pandas(df)

        with gcs.open_output_stream(f"{GCS_BUCKET}/{gcs_path}") as f:
            pq.write_table(table, f)

        rows_done += len(df)
        chunk_idx += 1

    elapsed = time.time() - t0
    log(f"✅ Concluído. {rows_done} linhas, {chunk_idx} arquivos. Tempo: {elapsed:.1f}s")

if __name__ == "__main__":
    try:
        export_full_table_to_gcs()
    except Exception as e:
        log(f"❌ Falha: {repr(e)}")
        raise
