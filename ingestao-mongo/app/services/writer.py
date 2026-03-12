from datetime import datetime
import os
from io import BytesIO
import pytz
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import logging
from google.api_core.retry import Retry
import time
import random
import requests
from google.api_core import exceptions as gexc
from google.auth.exceptions import TransportError


logger = logging.getLogger("mongo_to_gcs.writer")

_storage_client = storage.Client()

def get_storage_client():
    return _storage_client

def dataframe_to_parquet_gcs(
    df: pd.DataFrame,
    bucket_name: str,
    prefix: str,
    file_prefix: str,
    ingest_ts,
    run_id: str
):
    client = get_storage_client()
    bucket = client.bucket(bucket_name)

    # file_suffix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    partition = ingest_ts.date().isoformat()

    df["dt_ingestao"] = ingest_ts
    df['dt_ingestao'] = pd.to_datetime(df['dt_ingestao']).dt.tz_localize(None).astype("datetime64[ns]")
    df["id_execucao"] = run_id

    # Construção do schema fixo (todos string + técnicos)
    fields = [pa.field(col, pa.string()) for col in sorted(df.columns)
              if col not in ["dt_ingestao", "id_execucao"]]

    fields.append(pa.field("dt_ingestao", pa.timestamp("ns")))
    fields.append(pa.field("id_execucao", pa.string()))

    schema = pa.schema(fields)

    # Cria tabela Arrow respeitando schema fixo
    table = pa.Table.from_pandas(
        df,
        schema=schema,
        preserve_index=False
    )

    # Caminho final no GCS
    file_name = f"{file_prefix}.parquet"
    blob_path = f"{prefix}/dt={partition}/{run_id}/{file_name}"
    blob = bucket.blob(blob_path)

    # Serialização em Parquet
    buffer = BytesIO()
    # df.to_parquet(buffer, index=False, compression="snappy")
    pq.write_table(table, buffer, compression="snappy", coerce_timestamps='us')
    # 🔒 Retry manual robusto
    max_attempts = 5
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            buffer.seek(0)
            blob.upload_from_file(
                buffer,
                content_type="application/octet-stream",
                timeout=1200,
            )

            logger.info(f"Parquet salvo em: gs://{bucket_name}/{blob_path}")
            return blob_path

        except (
            requests.exceptions.ConnectionError,
            gexc.ServiceUnavailable,
            gexc.TooManyRequests,
            gexc.InternalServerError,
            gexc.DeadlineExceeded,
            TransportError,
        ) as e:

            last_exception = e

            sleep_time = min(60, 2 ** attempt) + random.random()
            logger.warning(
                f"Tentativa {attempt} falhou ao enviar {blob_path}. "
                f"Aguardando {sleep_time:.1f}s"
            )
            time.sleep(sleep_time)

    # Se chegou aqui, falhou mesmo
    logger.error(f"Falha definitiva no upload de {blob_path}")
    raise last_exception
