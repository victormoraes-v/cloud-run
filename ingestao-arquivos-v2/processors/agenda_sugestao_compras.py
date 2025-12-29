import logging

def custom_transform(df, file_name):
    logging.info(f"Aplicando transformação customizada no arquivo {file_name}")

    df.drop_duplicates(subset=['GRUPO_COMPRA'], inplace=True)

    logging.info(f"Transformação aplicada no arquivo {file_name}")

    return df