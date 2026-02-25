import json
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from bson import Decimal128, ObjectId

logger = logging.getLogger("mongo_to_gcs.transformer")


def normalize_documents(documents):
    if not documents:
        return pd.DataFrame()

    # df = pd.json_normalize(documents)
    df = pd.toDataFrame(documents)
    logger.info(f"Normalização concluída — linhas={len(df)} colunas={len(df.columns)}")

    df = _sanitize_column_names(df)

    for col in df.columns:
        df[col] = df[col].apply(_normalize_scalar_for_parquet)

    _ensure_arrow_friendly_types(df)
    return df


def _normalize_scalar_for_parquet(value: Any):
    if isinstance(value, ObjectId):
        return str(value)

    if isinstance(value, Decimal128):
        return value.to_decimal()

    if isinstance(value, Decimal):
        return value

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)

    return value


def _ensure_arrow_friendly_types(df: pd.DataFrame):
    """
    Converte colunas com tipos mistos em strings para evitar ArrowTypeError.
    """
    for col in df.columns:
        series = df[col]
        if series.dtype == "object":
            unique_types = {type(v) for v in series.dropna()}
            if len(unique_types) > 1:
                df[col] = series.astype(str)


def _sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove caracteres problemáticos (., $, espaços) dos nomes das colunas para
    compatibilidade com BigQuery e Parquet.
    """
    safe_columns = []
    for col in df.columns:
        safe = col.replace(".", "_").replace("$", "_").replace(" ", "_")
        safe_columns.append(safe)

    df.columns = safe_columns
    return df