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
        df_pedidos = pd.read_excel(f, engine='openpyxl', sheet_name='Pedidos')

    with smbclient.open_file(file_path, mode="rb", username=username, password=password) as f:
        df_pedidos_produtos = pd.read_excel(f, engine='openpyxl', sheet_name='Pedidos_Produtos')

    df_pedidos = normalize_column_names(df_pedidos)
    df_pedidos = add_ingestion_timestamp(df_pedidos)

    df_pedidos_produtos = normalize_column_names(df_pedidos_produtos)
    df_pedidos_produtos = add_ingestion_timestamp(df_pedidos_produtos)

    out_name_pedidos = f"{file_name}.csv"
    full_path_pedidos = write_dataframe_to_gcs(
        df_pedidos,
        out_name_pedidos,
        bucket_name,
        folder_path=destination_path.rsplit("/", 1)[0] + "/"
    )

    out_name_pedidos_produtos = f"{file_name}_produtos.csv"
    full_path_produtos = write_dataframe_to_gcs(
        df_pedidos_produtos,
        out_name_pedidos_produtos,
        bucket_name,
        folder_path=destination_path.rsplit("/", 1)[0] + "/"
    )

    logging.info(f"Arquivos salvos → {full_path_pedidos} e {full_path_produtos}")