from google.cloud import storage
import logging

def write_dataframe_to_gcs(df, file_name: str, destination_bucket_name: str, folder_path: str = "api/") -> str:
    """
    Converte um DataFrame em CSV e salva no GCS.
    Retorna o caminho completo gs://...
    """
    storage_client = storage.Client()

    destination_file_name = f"{folder_path}{file_name}.csv"
    destination_bucket = storage_client.bucket(destination_bucket_name)
    destination_blob = destination_bucket.blob(destination_file_name)

    csv_data = df.to_csv(sep=',', index=False, encoding='utf-8')
    destination_blob.upload_from_string(csv_data, content_type='text/csv')

    full_path = f"gs://{destination_bucket_name}/{destination_file_name}"
    logging.info(f"DataFrame salvo com sucesso em: {full_path}")
    return full_path