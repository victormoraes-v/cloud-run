import logging
import pandas as pd
import smbclient
from pathlib import Path
from utils import (
    normalize_column_names,
    add_ingestion_timestamp,
    build_destination_path,
    write_dataframe_to_gcs_parquet,
    delete_partition_for_file,
)


def process(file_path, username, password, bucket_name, file_to_process, file_format="parquet", write_mode="partitioned", chunksize=500_000):
    logging.info(f"Lendo arquivo CSV/TXT em chunks do SMB: {file_path}")

    file_name = Path(file_to_process).stem
    if 'RPE_117' in file_name:
        folder_path = 'mensal'
    if 'FF_107' in file_name:
        folder_path = 'semanal'
        
    destination_path = build_destination_path(file_to_process, f"arquivos/iqvia/{folder_path}/", write_mode, file_format)
    delete_partition_for_file(bucket_name, destination_path)

    with smbclient.open_file(file_path, mode="rb", username=username, password=password) as f:
        reader = pd.read_csv(f, sep=";", encoding="latin1", chunksize=chunksize, header=0)

        for i, chunk in enumerate(reader, 1):
            logging.info(f"Chunk {i} lido: {chunk.shape[0]} linhas")

            chunk = normalize_column_names(chunk)
            chunk = add_ingestion_timestamp(chunk)

            out_name = f"{file_name}_part{i}.parquet"
            full_path = write_dataframe_to_gcs_parquet(
                chunk,
                out_name,
                bucket_name,
                folder_path=destination_path.rsplit("/", 1)[0] + "/"
            )

            logging.info(f"Chunk {i} salvo → {full_path}")

    logging.info("Processor CSV concluído")
