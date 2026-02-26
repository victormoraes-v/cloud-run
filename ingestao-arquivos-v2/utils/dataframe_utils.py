import pandas as pd
from datetime import datetime
import pytz
import unicodedata
import re
import numpy as np

def _remove_accents_and_handle_cedilla(text):
    """
    Remove acentos de caracteres, trata 'ç' para 'c', e remove outros caracteres especiais,
    mantendo espaços.

    Args:
        text (str): A string de entrada.

    Returns:
        str: A string com acentos removidos, 'ç' tratado, e outros caracteres especiais limpos.
    """
    # 1. Normalizar para NFD (Normalization Form D) para separar a base do caractere do diacrítico
    # Ex: 'á' se torna 'a' + '\u0301' (combinando diacrítico)
    normalized_text = unicodedata.normalize('NFD', text)

    # 2. Filtrar os caracteres, mantendo apenas os que não são diacríticos
    # e reconstruindo a string. Ignora os caracteres de categoria 'Mn' (Mark, Nonspacing).
    without_accents = "".join([
        char for char in normalized_text if unicodedata.category(char) != 'Mn'
    ])

    # 3. Tratar 'ç' para 'c' (e 'Ç' para 'C')
    # Fazemos isso após a remoção de acentos para evitar problemas com 'ç' sendo tratado como 'c' + acento.
    # Usamos replace para garantir que 'ç' e 'Ç' sejam tratados.
    handled_cedilla = without_accents.replace('ç', 'c').replace('Ç', 'C')

    # 4. Remover quaisquer outros caracteres especiais que não sejam letras, números ou espaços.
    # O padrão r"[^a-zA-Z0-9 ]" significa: qualquer coisa que NÃO seja (a-z, A-Z, 0-9, ou um espaço).
    # Isso garante que apenas letras, números e espaços sejam mantidos.
    cleaned_text = re.sub(r"[^a-zA-Z0-9_ ]", "", handled_cedilla)

    return cleaned_text

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza os nomes das colunas:
    - remove espaços extras
    - substitui espaços internos por underscores
    - deixa tudo em maiúsculas
    """
    df.columns = (
        df.columns
        .map(_remove_accents_and_handle_cedilla)
        .str.strip()               # remove espaços no início/fim
        .str.replace(r'\s+', '_', regex=True)  # troca espaços por _
        .str.upper()               # tudo maiusculo
    )
    return df

def add_ingestion_timestamp(df: pd.DataFrame, tz: str = "America/Sao_Paulo") -> pd.DataFrame:
    """
    Adiciona uma coluna dt_ingestao com timestamp atual (sem timezone).
    - tz: fuso horário a ser usado (default: America/Sao_Paulo)
    """
    current_ts = datetime.now(pytz.timezone(tz)).replace(tzinfo=None)
    df["DT_INGESTAO"] = current_ts
    return df

def normalize_dataframe_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    # 1️⃣ Converte tipos automaticamente (usa pandas nullable dtypes)
    df = df.convert_dtypes()

    # 2️⃣ Para qualquer coluna ainda object, força string
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype("string")

    # 3️⃣ Opcional: substituir np.nan por None (mais seguro pro parquet)
    df = df.replace({np.nan: None})

    return df