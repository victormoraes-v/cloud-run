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
        df = pd.read_excel(f, engine='openpyxl', header=4)

    df = normalize_column_names(df)
    df = add_ingestion_timestamp(df)

    out_name = f"{file_name}.csv"
    full_path = write_dataframe_to_gcs(
        df,
        out_name,
        bucket_name,
        folder_path=destination_path.rsplit("/", 1)[0] + "/"
    )

    logging.info(f"Arquivo salvo → {full_path}")