import os
import gzip
import pandas as pd
from google.cloud import storage
from io import BytesIO, TextIOWrapper
import csv
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sys
from google.cloud import bigquery

load_dotenv()

BUCKET_NAME = os.environ.get('BUCKET_NAME')
PREFIX = os.environ.get('PREFIX')
PROJECT_ID = os.environ.get('PROJECT_ID')
DATASET_BQ = os.environ.get('DATASET_BQ')
TABELA_BQ = os.environ.get('TABELA_BQ')

RUN = os.environ.get('RUN')

def cast_to_bq_schema(df: pd.DataFrame, project_id: str, dataset: str, table: str) -> pd.DataFrame:
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset).table(table)
    table_obj = client.get_table(table_ref)

    bq_schema = {field.name: field.field_type for field in table_obj.schema}

    for col in df.columns:
        if col in bq_schema:
            bq_type = bq_schema[col]
            try:
                if bq_type == "INTEGER":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif bq_type in ["FLOAT", "NUMERIC", "BIGNUMERIC"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif bq_type == "BOOLEAN":
                    df[col] = df[col].astype(str).str.lower().map({"true": True, "false": False})
                elif bq_type in ["DATE", "DATETIME", "TIMESTAMP"]:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                else:  # STRING, BYTES etc
                    df[col] = df[col].astype(str)
            except Exception as e:
                print(f"[WARN] Erro ao converter coluna {col} para {bq_type}, salvando como string. Erro: {e}")
                df[col] = df[col].astype(str)
        else:
            print(f"[INFO] Coluna nova detectada: {col}, salvando como STRING")
            df[col] = df[col].astype(str)

    return df


storage_client = storage.Client()

def run_job(event='', context=None):
    """
    Pode ser chamado tanto por trigger do GCS quanto manualmente (Airflow).
    - Se vier do GCS: usa event["bucket"] e event["name"]
    - Se vier do Airflow: espera {"bucket": "...", "prefix": "..."}
    """
    # Se veio de evento GCS
    if "bucket" in event and "name" in event:
        bucket_name = event["bucket"]
        prefix = os.path.dirname(event["name"]) + "/"

    # Se veio do Airflow (chamada manual)
    elif BUCKET_NAME and PREFIX:
        bucket_name = BUCKET_NAME
        current_date = datetime.now() - timedelta(days=3)
        run_id = RUN if RUN else datetime.strftime(current_date, '%Y-%m-%d') 
        prefix = f'{PREFIX}/run={run_id}'

    else:
        raise ValueError("Parâmetros inválidos. Esperado {bucket,name} ou {bucket,prefix}")

    print(f"Processando todos os arquivos em: gs://{bucket_name}/{prefix}")
    # Lista todos os objetos da pasta
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    for blob in blobs:
        if not blob.name.endswith(".gz"):
            print(f"Ignorando {blob.name} (não é .gz)")
            continue

        print(f"Lendo {blob.name}...")

        # Baixa o arquivo .gz em memória
        gz_bytes = blob.download_as_bytes()

        # Descompacta e lê em DataFrame (assumindo CSV dentro do .gz)
        with gzip.GzipFile(fileobj=BytesIO(gz_bytes)) as gz:
            text_stream = TextIOWrapper(gz, encoding="utf-8")
            df = pd.read_csv(text_stream, sep='|', encoding='utf-8', quoting=csv.QUOTE_NONE)

        parquet_file = blob.name.replace('.gz', '.parquet').replace('transient', 'landing')
        parquet_buffer = BytesIO()

        df = cast_to_bq_schema(df, PROJECT_ID, DATASET_BQ, TABELA_BQ)
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")

        # Upload para bucket de saída
        out_bucket = storage_client.bucket(bucket_name)
        out_blob = out_bucket.blob(parquet_file)
        out_blob.upload_from_file(parquet_buffer, content_type="application/octet-stream", rewind=True)

        print(f"✔ Convertido: gs://{out_bucket}/{parquet_file}")

    
if __name__ == "__main__":
    sys.exit(run_job())