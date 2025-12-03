# db/sqlserver_loader.py
"""
Implementação do loader para SQL Server.
Contém apenas lógica específica do SQL Server.
"""

import json
from typing import List, Tuple, Optional
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from .base_loader import BaseLoader


# Mapa de conversão dos tipos do SQL Server para tipos compatíveis com BigQuery
SQLSERVER_TYPE_MAP = {
    'varchar': 'STRING', 'nvarchar': 'STRING', 'char': 'STRING', 'text': 'STRING', 'nchar': 'STRING',
    'ntext': 'STRING', 'xml': 'STRING', 'uniqueidentifier': 'STRING',
    'int': 'INT64', 'bigint': 'INT64', 'smallint': 'INT64', 'tinyint': 'INT64',
    'bit': 'BOOL', 'decimal': 'NUMERIC', 'numeric': 'NUMERIC', 'money': 'NUMERIC', 'smallmoney': 'NUMERIC',
    'float': 'FLOAT64', 'real': 'NUMERIC', 'date': 'DATE', 'datetime': 'DATETIME', 'datetime2': 'DATETIME',
    'smalldatetime': 'DATETIME', 'time': 'TIME', 'datetimeoffset': 'TIMESTAMP',
    'binary': 'BYTES', 'varbinary': 'BYTES', 'image': 'BYTES', 'timestamp': 'INT64'
}


class SqlServerLoader(BaseLoader):
    """Implementação concreta do loader para SQL Server."""

    def get_engine(self, conn_json: str):
        """
        Constrói a engine SQLAlchemy com base no JSON de credenciais usado no Cloud Run.
        """
        db = json.loads(conn_json)['connections'][0]
        pwd = quote_plus(db['database_password'])

        url = (
            f"mssql+pymssql://{db['database_username']}:{pwd}@"
            f"{db['database_hostname']}:{db['database_port']}/"
            f"{db['database_name']}"
        )
        return create_engine(url)

    def get_schema(self, eng, table: str) -> List[Tuple[str, str]]:
        """
        Lê o schema da tabela usando INFORMATION_SCHEMA do SQL Server.
        """
        parts = table.split('.')
        schema_name = parts[0] if len(parts) > 1 else 'dbo'
        tbl_name = parts[1] if len(parts) > 1 else table

        q = text(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}'
              AND TABLE_NAME = '{tbl_name}'
            ORDER BY ORDINAL_POSITION
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
        Gera o SELECT com conversão segura para BigQuery (CAST col AS STRING, INT64, etc).
        """
        clauses = []

        if partition_col:
            clauses.append(f"CAST({partition_col} AS DATE) AS DT")

        for col, typ in schema:
            bq_type = SQLSERVER_TYPE_MAP.get(typ, "STRING")
            clauses.append(f"CAST({col} AS {bq_type}) AS {col}")

        clauses.append("CAST(DT_INGESTAO AS DATETIME) AS DT_INGESTAO")
        clauses.append("ID_EXECUCAO")

        indent = "  "
        sep = f",\n{indent}"
        return f"SELECT\n{indent}{sep.join(clauses)}\nFROM ${{ref('{table}')}}"
