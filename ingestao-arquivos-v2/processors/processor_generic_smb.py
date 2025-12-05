import logging
import pandas as pd
import smbclient
from importlib import import_module
from utils import (
    normalize_column_names,
    add_ingestion_timestamp,
    read_file_from_smb,
    save_to_gcs
)

def process_generic(cfg: dict, file_path: str, username: str, password: str, bucket_name: str):
    """
    Processa qualquer arquivo (xlsx, csv, txt, parquet) via configuração dinâmica vinda do BigQuery.

    Fluxo:
      1. Leitura do arquivo via SMB (chunk ou único)
      2. Normalização opcional de colunas
      3. Adição opcional do ingestion_timestamp
      4. Aplicação opcional do processador customizado (custom_processor)
      5. Escrita no GCS
    """

    with smbclient.open_file(file_path, mode="rb", username=username, password=password) as f:
        data = read_file_from_smb(f, cfg)

        # 2. Se chunksize definido → processar por chunks
        if hasattr(data, "__iter__") and cfg.get("chunksize"):
            logging.info(f"Processando em chunks (tamanho={cfg.get('chunksize')})")

            for index, chunk in enumerate(data):
                logging.info(f"Processando chunk #{index + 1}")

                chunk = _apply_common_transformations(chunk, cfg)
                chunk = _apply_custom_transformation(chunk, cfg)

                save_to_gcs(
                    df=chunk,
                    bucket_name=bucket_name,
                    folder_path=cfg.get("destination_folder", "arquivos/"),
                    write_mode=cfg.get("write_mode", "partitioned"),
                    file_format=cfg.get("output_file_format", "parquet")
                )

            logging.info(f"Processamento em chunks finalizado: {cfg.get('file_name')}")
            return

    # 3. Dataframe único (sem chunks)
    df = data

    df = _apply_common_transformations(df, cfg)
    df = _apply_custom_transformation(df, cfg)

    # 4. SALVAR RESULTADO
    save_to_gcs(
        df=df,
        bucket_name=bucket_name,
        folder_path=cfg.get("destination_folder", "arquivos/"),
        write_mode=cfg.get("write_mode", "overwrite"),
        file_format=cfg.get("output_file_format", "csv")
    )

    logging.info(f"Processamento concluído: {cfg.get('file_name')}")



# ===============================================================
# 🔧 MÉTODOS AUXILIARES
# ===============================================================

def _apply_common_transformations(df, cfg):
    """Aplica normalização de colunas e timestamp."""
    df = normalize_column_names(df)
    df = add_ingestion_timestamp(df)
    return df


def _apply_custom_transformation(df, cfg):
    """Executa transformações específicas se houver um módulo customizado."""
    custom_processor = cfg.get("custom_processor")

    if custom_processor:
        logging.info(f"Aplicando transformação customizada: {custom_processor}")
        module = import_module(f"processors.{custom_processor}")
        df = module.custom_transform(df)

    return df