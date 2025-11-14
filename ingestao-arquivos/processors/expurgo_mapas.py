import logging
import pandas as pd
import smbclient
from pathlib import Path
from utils import (
    normalize_column_names,
    add_ingestion_timestamp,
    build_destination_path,
    write_dataframe_to_gcs
)

def process(file_path, username, password, bucket_name, file_to_process, file_format="csv", write_mode="overwrite"):
    logging.info(f"Lendo arquivo CSV/TXT em chunks do SMB: {file_path}")

    file_name = Path(file_to_process).stem
    destination_path = build_destination_path(file_to_process, f"arquivos/", write_mode, file_format)

    with smbclient.open_file(file_path, mode="rb", username=username, password=password) as f:
        df_mapas = pd.read_excel(f, engine='openpyxl', sheet_name='Expurgo_Mapas')
        df_romaneios = pd.read_excel(f, engine='openpyxl', sheet_name='Expurgo_Romaneios')

    df_mapas = normalize_column_names(df_mapas)
    df_mapas = add_ingestion_timestamp(df_mapas)
    df_romaneios = normalize_column_names(df_romaneios)
    df_romaneios = add_ingestion_timestamp(df_romaneios)

    out_name_mapas = f"{file_name}_mapas.csv"
    out_name_romaneios = f"{file_name}_romaneios.csv"

    full_path_mapas = write_dataframe_to_gcs(
        df_mapas,
        out_name_mapas,
        bucket_name,
        folder_path=destination_path.rsplit("/", 1)[0] + "/"
    )

    full_path_romaneios = write_dataframe_to_gcs(
        df_romaneios,
        out_name_romaneios,
        bucket_name,
        folder_path=destination_path.rsplit("/", 1)[0] + "/"
    )

    logging.info(f"Arquivo salvos → {full_path_mapas} e {full_path_romaneios}")

