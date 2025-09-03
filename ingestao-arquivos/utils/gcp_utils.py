from google.cloud import secretmanager
from google.cloud import storage
from pathlib import Path
import logging

secret_client = secretmanager.SecretManagerServiceClient()

def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """
    Busca o valor de um secret no Google Secret Manager.
    """
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = secret_client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

storage_client = storage.Client()

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