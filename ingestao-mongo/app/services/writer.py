from datetime import datetime
import os
from io import BytesIO
import pytz
import pandas as pd
from google.cloud import storage
import logging

logger = logging.getLogger("mongo_to_gcs.writer")

def dataframe_to_parquet_gcs(df: pd.DataFrame, bucket_name: str, prefix: str, file_prefix: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    tz = pytz.timezone('America/Sao_Paulo')
    now = datetime.now(tz)
    file_suffix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    partition = now.strftime("%Y-%m-%d")

    df["dt_ingestao"] = now

    # Caminho final no GCS
    file_name = f"{file_prefix}_{file_suffix}.parquet"
    blob_path = f"{prefix}/dt={partition}/{file_name}"
    blob = bucket.blob(blob_path)

    # Serialização em Parquet
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)

    # Upload
    blob.upload_from_file(buffer, content_type="application/octet-stream", timeout=1200)

    logger.info(f"Parquet salvo em: gs://{bucket_name}/{blob_path}")
    return blob_path
