from typing import Dict, Any
from datetime import datetime
import logging

from ..models import CollectionConfig

logger = logging.getLogger("mongo_to_gcs.extractor")


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
