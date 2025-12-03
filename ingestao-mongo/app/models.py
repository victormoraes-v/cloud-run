from dataclasses import dataclass
from typing import Dict, List, Optional, Any


@dataclass
class CollectionConfig:
    name: str                          # Nome lógico (ex: 'customers')
    db_name: str                       # Nome do DB no Mongo
    collection_name: str              # Nome da coleção no Mongo
    projection: List[str]             # Campos a projetar
    types: Dict[str, str]             # Mapeamento campo -> tipo lógico ('str', 'date', etc.)
    dedupe_keys: List[str]            # Colunas para remover duplicadas (ex: ['document'])
    sort_field: str                   # Campo para ordenar antes de deduplicar (ex: 'updatedAt')
    filter_deleted_field: Optional[str] = None   # Ex: 'deleted'
    filter_deleted_value: Optional[Any] = False  # Ex: False
    incremental_field: Optional[str] = None      # Ex: 'updatedAt'
    incremental_start_ts: Optional[str] = None   # Ex: '2020-01-01T00:00:00Z'
    gcs_output_prefix: str = ""       # Prefixo no GCS (ex: 'mongo/customers/')
