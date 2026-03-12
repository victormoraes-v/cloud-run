from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging
from datetime import datetime
from ..config.constants import DEFAULT_WATERMARK

logger = logging.getLogger("mongo_to_gcs.extractor")

def _ensure_datetime(value):
    """
    Garante que o valor seja datetime.
    Aceita:
    - datetime
    - string no formato YYYY-MM-DD
    """

    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        return datetime.strptime(value, "%Y-%m-%d")

    raise ValueError(f"Formato de data inválido: {value}")

def get_max_date_from_bq_table(
    bq: bigquery.Client,
    project_id: str,
    target_dataset: str,
    target_table: str,
):

    sql = f"""
        SELECT
            CAST(DATE_ADD(MAX(DT), INTERVAL -1 DAY) AS TIMESTAMP) AS LAST_DATETIME
        FROM `{project_id}.{target_dataset}.{target_table}`
    """

    try:
        result = bq.query(sql).result()
        row = next(result, None)

        if not row or not row["LAST_DATETIME"]:
            return DEFAULT_WATERMARK

        return row["LAST_DATETIME"]

    except NotFound:
        return DEFAULT_WATERMARK

    except Exception:
        logger.exception(f"Erro ao buscar watermark no BigQuery. Utilizando {DEFAULT_WATERMARK}")
        return DEFAULT_WATERMARK

# def build_mongo_query(incremental_field: str, incremental_timestamp):
#     """Monta query incremental padrão."""
#     return {
#         "$expr": {
#             "$gte": [
#                 {"$toDate": f"${incremental_field}"},
#                 incremental_timestamp
#             ]
#         }
#         , 'deleted': False
#     }

def build_mongo_query(
    incremental_field: str,
    start_date,
    end_date=None
):
    """
    Constrói query incremental para MongoDB com suporte a janela de datas.

    Aceita start_date e end_date como:
    - datetime
    - string no formato 'YYYY-MM-DD'

    Regras:
    - field >= start_date
    - field < end_date (se informado)
    - deleted = False
    """

    start_date = _ensure_datetime(start_date)
    end_date = _ensure_datetime(end_date)

    date_conditions = [
        {
            "$gte": [
                {"$toDate": f"${incremental_field}"},
                start_date
            ]
        }
    ]

    if end_date:
        date_conditions.append(
            {
                "$lt": [
                    {"$toDate": f"${incremental_field}"},
                    end_date
                ]
            }
        )

    return {
        "$expr": {
            "$and": date_conditions
        },
        "deleted": False
    }

def build_projection(projection_list: list):
    """Converte lista ['a','b','c'] → {'a':1,'b':1,'c':1} para Mongo."""
    return {field: 1 for field in projection_list}
