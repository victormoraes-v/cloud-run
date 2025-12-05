from google.cloud import secretmanager
from google.cloud import storage
from pathlib import Path
import logging
import pytz
from datetime import datetime
import uuid
from io import BytesIO
import pandas as pd
from google.cloud import bigquery

secret_client = secretmanager.SecretManagerServiceClient()

def delete_partition_for_file(bucket_name: str, destination_path: str):
    """
    Deleta todos os arquivos dentro da partição (dt=YYYY-MM-DD) de um arquivo específico.
    
    Args:
        bucket_name (str): Nome do bucket GCS.
        destination_path (str): Caminho completo até o arquivo que será salvo,
                                ex: "arquivos/meu_arquivo/dt=2025-09-17/part1.parquet"
    """
    # Extrai só a "pasta da partição"
    partition_prefix = destination_path.rsplit("/", 1)[0] + "/"

    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=partition_prefix)

    deleted_files = 0
    for blob in blobs:
        logging.info(f"Deletando {blob.name} do bucket {bucket_name}")
        blob.delete()
        deleted_files += 1

    if deleted_files == 0:
        logging.info(f"Nenhum arquivo encontrado para deletar em {partition_prefix}")
    else:
        logging.info(f"{deleted_files} arquivos deletados em {partition_prefix}")

def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """
    Busca o valor de um secret no Google Secret Manager.
    """
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = secret_client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

storage_client = storage.Client()

def get_migration_table_config(table_config, file_name):
    client = bigquery.Client()
    
    query = f"""
    SELECT *
    FROM data_migration_config.{table_config}
    WHERE file_name = @file_name AND active = TRUE
    """
    result = list(client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("file_name", "STRING", file_name)]
    )))


    if not result:
        raise ValueError(
            f"📌 Arquivo '{file_name}' não está configurado na tabela '{table_config}'.\n"
            "🛑 Nenhum processamento foi executado.\n"
            "💡 Para habilitar este arquivo, insira uma linha na tabela com todas as configurações necessárias.\n"
            "👉 Exemplo:\n"
            f"INSERT INTO data_migration_config.{table_config} (...campos...) VALUES (...);\n"
        )

    # Converte Row para dict e coloca chaves em minúsculo
    row_dict = {k.lower(): v for k, v in dict(result[0]).items()}

    return row_dict

def build_destination_path(original_file_name: str, folder_path: str, write_mode: str, file_format: str) -> str:
    """
    Monta o caminho do arquivo no GCS de acordo com o write_mode.
    - overwrite → sempre substitui
    - partitioned → cria pasta dt=yyyy-MM-dd
    """
    stem = Path(original_file_name).stem
    today = datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%Y-%m-%d")

    if write_mode == "partitioned":
        return f"{folder_path}{stem}/dt={today}/{stem}.{file_format}"
    else:  # overwrite
        return f"{folder_path}{stem}.{file_format}"

def write_dataframe_to_gcs(df, original_file_name: str, destination_bucket_name: str, folder_path: str = "arquivos/") -> str:
    """
    Converte um DataFrame em CSV e salva no GCS.
    Retorna o caminho completo gs://...
    """
    destination_file_name = f"{folder_path}{Path(original_file_name).stem}.csv"
    destination_bucket = storage_client.bucket(destination_bucket_name)
    destination_blob = destination_bucket.blob(destination_file_name)

    csv_data = df.to_csv(sep=',', index=False, encoding='utf-8')
    destination_blob.upload_from_string(csv_data, content_type='text/csv')

    full_path = f"gs://{destination_bucket_name}/{destination_file_name}"
    logging.info(f"DataFrame salvo com sucesso em: {full_path}")
    return full_path

def write_dataframe_to_gcs_parquet(df, original_file_name: str, destination_bucket_name: str, folder_path: str = "arquivos/") -> str:
    """
    Converte um DataFrame em Parquet e salva no GCS.
    Retorna o caminho completo gs://...
    """
    destination_file_name = f"{folder_path}{Path(original_file_name).stem}.parquet"
    destination_bucket = storage_client.bucket(destination_bucket_name)
    destination_blob = destination_bucket.blob(destination_file_name)

    # Força upload resumível
    destination_blob.chunk_size = 8 * 1024 * 1024  # 8MB (pode subir para 16/32MB se quiser)

    # Salvar DataFrame em buffer de memória no formato parquet
    import io
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)

    # Upload para GCS
    destination_blob.upload_from_file(
        parquet_buffer, 
        content_type="application/octet-stream",  # ou "application/x-parquet"
        timeout=1200
    )

    full_path = f"gs://{destination_bucket_name}/{destination_file_name}"
    logging.info(f"DataFrame salvo com sucesso em: {full_path}")
    return full_path

def save_to_gcs(
    df: pd.DataFrame,
    bucket_name: str,
    folder_path: str = "arquivos/",
    write_mode: str = "overwrite",
    file_format: str = "csv",
    original_file_name: str | None = None
):
    """
    Salva DataFrame no GCS de acordo com o modo de escrita:

      ✔ overwrite   → substitui um único arquivo (nome fixo)
      ✔ append      → cria arquivos adicionais (UUID)
      ✔ partitioned → cria pastas baseadas na data de carga (dt=YYYY-MM-DD)

    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # ===========================================================
    # 1) Nome do arquivo
    # ===========================================================
    if write_mode == "overwrite" and original_file_name:
        final_name = f"{Path(original_file_name).stem}.{file_format}"
    else:
        final_name = f"{uuid.uuid4()}.{file_format}"

    # ===========================================================
    # 2) Monta path de destino com ou sem dt=
    # ===========================================================
    base_path = folder_path.rstrip("/")

    if write_mode == "partitioned":
        today = datetime.utcnow().strftime("%Y-%m-%d")
        blob_path = f"{base_path}/dt={today}/{final_name}"
    else:
        blob_path = f"{base_path}/{final_name}"

    blob = bucket.blob(blob_path)

    # ===========================================================
    # 3) Serialização
    # ===========================================================
    buffer = BytesIO()

    if file_format == "csv":
        df.to_csv(buffer, index=False, encoding="utf-8")
        buffer.seek(0)
        content_type = "text/csv"

    elif file_format == "parquet":
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        content_type = "application/octet-stream"

    else:
        raise ValueError(f"Formato não suportado: {file_format}")

    # ===========================================================
    # 4) Upload
    # ===========================================================
    blob.upload_from_file(buffer, content_type=content_type)

    full_path = f"gs://{bucket_name}/{blob_path}"
    logging.info(f"🚀 Arquivo salvo em: {full_path}")

    return full_path