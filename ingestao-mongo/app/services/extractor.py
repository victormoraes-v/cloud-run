from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging

from ..config.constants import DEFAULT_WATERMARK

logger = logging.getLogger("mongo_to_gcs.extractor")

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

def build_mongo_query(incremental_field: str, incremental_timestamp):
    """Monta query incremental padrão."""
    return {
        "$expr": {
            "$gte": [
                {"$toDate": f"${incremental_field}"},
                incremental_timestamp
            ]
        }
        , 'deleted': False
    }

def build_projection(projection_list: list):
    """Converte lista ['a','b','c'] → {'a':1,'b':1,'c':1} para Mongo."""
    return {field: 1 for field in projection_list}
