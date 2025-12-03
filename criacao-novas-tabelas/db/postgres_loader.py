# db/postgres_loader.py
"""
Implementação do loader para PostgreSQL.
Contém apenas lógica específica do PostgreSQL.
"""

import json
from typing import List, Tuple, Optional
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from .base_loader import BaseLoader


# Mapa de conversão dos tipos do PostgreSQL para tipos compatíveis com BigQuery
POSTGRES_TYPE_MAP = {
    'character varying': 'STRING', 'varchar': 'STRING', 'text': 'STRING',
    'integer': 'INT64', 'bigint': 'INT64', 'smallint': 'INT64',
    'boolean': 'BOOL',
    'numeric': 'NUMERIC', 'double precision': 'FLOAT64',
    'timestamp without time zone': 'DATETIME',
    'timestamp with time zone': 'TIMESTAMP',
    'bytea': 'BYTES',
    'uuid': 'STRING'
}


class PostgresLoader(BaseLoader):
    """Implementação concreta do loader para PostgreSQL."""

    def get_engine(self, conn_json: str):
        """
        Constrói a engine SQLAlchemy com base no JSON de credenciais usado no Cloud Run.
        """
        db = json.loads(conn_json)['connections'][0]
        pwd = quote_plus(db["database_password"])

        url = (
            f"postgresql+psycopg2://{db['database_username']}:{pwd}@"
            f"{db['database_hostname']}:{db['database_port']}/"
            f"{db['database_name']}"
        )
        return create_engine(url)

    def get_schema(self, eng, table: str) -> List[Tuple[str, str]]:
        """
        Lê o schema da tabela via INFORMATION_SCHEMA do PostgreSQL.
        """
        parts = table.split('.')
        schema_name = parts[0] if len(parts) > 1 else 'public'
        tbl_name = parts[1] if len(parts) > 1 else table

        q = text(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='{schema_name}'
              AND table_name='{tbl_name}'
            ORDER BY ordinal_position
        """)

        with eng.connect() as c:
            return [(r[0].upper(), r[1].lower()) for r in c.execute(q)]

    def generate_select_safe_cast(
        self,
        schema: List[Tuple[str, str]],
        table: str,
        partition_col: Optional[str] = None
    ) -> str:
        """
        Gera o SELECT com conversão segura para BigQuery (CAST col AS STRING, INT64...).
        """
        clauses = []

        if partition_col:
            clauses.append(f"{partition_col}::date AS DT")

        for col, typ in schema:
            bq_type = POSTGRES_TYPE_MAP.get(typ, "STRING")
            clauses.append(f"CAST({col.lower()} AS {bq_type}) AS {col}")

        clauses.append("DT_INGESTAO")
        clauses.append("ID_EXECUCAO")

        indent = "  "
        sep = f",\n{indent}"
        return f"SELECT\n{indent}{sep.join(clauses)}\nFROM ${{ref('{table}')}}"
