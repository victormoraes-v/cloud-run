import logging
import os
from datetime import datetime, date, timezone
from io import BytesIO
from typing import List
from datetime import datetime

from dotenv import load_dotenv
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv()

PROJECT_ID = os.environ.get("PROJECT_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
PREFIX = os.environ.get("PREFIX")
DATE = os.environ.get("DATE")
DATASET_BQ = os.environ.get("DATASET_BQ")
TABELA_BQ = os.environ.get("TABELA_BQ")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("ingestao-connectly")


def _resolve_process_date(raw_date: str | None) -> date:
    """
    Converte a string de data (yyyy-MM-dd) para date.
    Se vazio/nulo, usa a data atual em UTC.
    """
    if raw_date:
        raw_date = raw_date.strip()
    if raw_date:
        try:
            return datetime.strptime(raw_date, "%Y-%m-%d").date()
        except ValueError:
            logger.warning(
                "Data inválida em JOB_DATE=%s. Usando data atual em UTC.", raw_date
            )
    return datetime.now(timezone.utc).date()


def _list_input_blobs(client: storage.Client, bucket_name: str, prefix: str) -> List[storage.Blob]:
    bucket = client.bucket(bucket_name)
    blobs_iter = client.list_blobs(bucket_or_name=bucket, prefix=prefix)
    blobs = [b for b in blobs_iter if not b.name.endswith("/")]
    return blobs


def _load_blob_to_dataframe(blob: storage.Blob) -> pd.DataFrame | None:
    """
    Lê um blob do GCS em um DataFrame.
    Detecta formato simples por extensão:
      - .json / .jsonl → JSON lines
      - .csv / .txt    → CSV padrão (delimitador ',')
    Outros formatos são ignorados.
    """
    name_lower = blob.name.lower()
    content = blob.download_as_bytes()
    buffer = BytesIO(content)

    try:
        if name_lower.endswith(".json") or name_lower.endswith(".jsonl"):
            return pd.read_json(buffer, lines=True)
        if name_lower.endswith(".csv") or name_lower.endswith(".txt"):
            return pd.read_csv(buffer)
    except Exception:
        logger.exception("Falha ao ler blob %s. Ignorando arquivo.", blob.name)
        return None

    logger.warning("Formato não suportado para blob %s. Ignorando arquivo.", blob.name)
    return None

def _write_parquet(
    client: storage.Client,
    bucket_name: str,
    df: pd.DataFrame,
    process_date: date,
    folder_name: str,
    output_filename: str,
) -> str:
    """
    Escreve um arquivo Parquet em:
      landing/dt={yyyy-MM-dd}/{output_filename}
    """
    if df.empty:
        raise ValueError("DataFrame vazio - nada para escrever em Parquet.")

    bucket = client.bucket(bucket_name)
    date_str = process_date.strftime("%Y-%m-%d")

    blob_name = f"landing/{folder_name}/dt={date_str}/{output_filename}"
    blob = bucket.blob(blob_name)

    ingest_ts = datetime.now()
    df["dt_ingestao"] = ingest_ts
    df['dt_ingestao'] = pd.to_datetime(df['dt_ingestao']).dt.tz_localize(None).astype("datetime64[ns]")

    run_id = ingest_ts.strftime("%Y%m%dT%H%M%SZ")
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

    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy", coerce_timestamps='us')
    buffer.seek(0)

    blob.upload_from_file(
        buffer,
        content_type="application/octet-stream",
        timeout=1200,
    )

    logger.info("Parquet gerado em gs://%s/%s", bucket_name, blob_name)
    return blob_name


def run() -> int:
    """
    Cloud Run Job:
      - Lê arquivos de gs://BUCKET/transient/{yyyy-MM-dd}/
      - Consolida em um único DataFrame
      - Grava Parquet em gs://BUCKET/landing/dt={yyyy-MM-dd}/
    """
    project_id = os.environ.get("PROJECT_ID")
    bucket_name = os.environ.get("BUCKET_NAME")
    prefix = os.environ.get("PREFIX")
    date_env = os.environ.get("DATE")

    if not bucket_name:
        raise ValueError("Variável de ambiente BUCKET_NAME é obrigatória.")

    process_date = _resolve_process_date(date_env)
    date_str = process_date.strftime("%Y-%m-%d")

    logger.info(
        "Iniciando job ingestao-connectly | project=%s bucket=%s process_date=%s",
        project_id,
        bucket_name,
        date_str,
    )

    client = storage.Client(project=project_id) if project_id else storage.Client()

    input_prefix = f"{prefix}/{date_str}/"
    blobs = _list_input_blobs(client, bucket_name, input_prefix)

    if not blobs:
        logger.warning(
            "Nenhum arquivo encontrado em gs://%s/%s. Nada a processar.",
            bucket_name,
            input_prefix,
        )
        return 0

    logger.info("Encontrados %d arquivos em %s", len(blobs), input_prefix)

    for blob in blobs:
        logger.info("Lendo arquivo: %s", blob.name)
        df_part = _load_blob_to_dataframe(blob)
        if df_part is not None and not df_part.empty:
            # Nome do arquivo de origem (sem caminho)
            original_name = os.path.basename(blob.name)
            base_name, _ = os.path.splitext(original_name)
            output_filename = f"{base_name}.parquet"
            folder_name = prefix.replace('transient/', '')

            # Converte todas as colunas para string
            df_part = df_part.astype(str)

            logger.info(
                "Salvando arquivo de saída para %s como %s",
                blob.name,
                output_filename,
            )
            _write_parquet(client, bucket_name, df_part, process_date, folder_name, output_filename)

    logger.info("Job finalizado com sucesso.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())

