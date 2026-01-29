"""Implementação do loader para Oracle.

Contém apenas lógica específica do Oracle.
"""

import json
from typing import List, Tuple, Optional
from urllib.parse import quote_plus
import oracledb

from sqlalchemy import create_engine, text

from .base_loader import BaseLoader


# Mapa de conversão dos tipos do Oracle para tipos compatíveis com BigQuery
ORACLE_TYPE_MAP = {
    # Texto
    "varchar2": "STRING",
    "nvarchar2": "STRING",
    "char": "STRING",
    "nchar": "STRING",
    "clob": "STRING",
    "nclob": "STRING",

    # Numéricos
    "number": "NUMERIC",
    "binary_float": "FLOAT64",
    "binary_double": "FLOAT64",

    # Datas / horários
    "date": "DATETIME",
    "timestamp": "TIMESTAMP",
    "timestamp with local time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMP",

    # Binários
    "blob": "BYTES",
    "raw": "BYTES",
    "long raw": "BYTES",
}


class OracleLoader(BaseLoader):
    """Implementação concreta do loader para Oracle."""

    def get_engine(self, conn_json: str):
        """
        Constrói a engine SQLAlchemy com base no JSON de credenciais usado no Cloud Run.

        Assume o mesmo formato de credencial já utilizado para SQL Server / Postgres:
        {
          "connections": [
            {
              "database_username": "...",
              "database_password": "...",
              "database_hostname": "...",
              "database_port": "...",
              "database_name": "NOME_SERVICO_OU_SID"
            }
          ]
        }
        """
        oracledb.init_oracle_client(lib_dir="/opt/oracle/instantclient_21_13")
        
        db = json.loads(conn_json)["connections"][0]
        pwd = quote_plus(db["database_password"])

        # Para o Oracle via SQLAlchemy, normalmente usamos:
        # oracle+cx_oracle://usuario:senha@host:porta/servico
        # url = (
        #     f"oracle+cx_oracle://{db['database_username']}:{pwd}@"
        #     f"{db['database_hostname']}:{db['database_port']}/"
        #     f"{db['database_name']}"
        # )

        url = (
                f"oracle+oracledb://{db['database_username']}:{pwd}@"
                f"{db['database_hostname']}:{db['database_port']}/"
                f"?service_name={db['database_name']}"
            )

        return create_engine(url)

    def get_schema(self, eng, table: str) -> List[Tuple[str, str]]:
        """
        Lê o schema da tabela via dicionário de dados do Oracle.

        Se for informado "SCHEMA.TABELA", usa ambos; caso contrário assume o schema
        padrão do usuário.
        """
        parts = table.split(".")
        if len(parts) > 1:
            schema_name = parts[0].upper()
            tbl_name = parts[1].upper()
        else:
            schema_name = None  # usa o schema padrão do usuário
            tbl_name = table.upper()

        if schema_name:
            q = text(
                """
                SELECT COLUMN_NAME, DATA_TYPE
                  FROM ALL_TAB_COLUMNS
                 WHERE OWNER = :schema_name
                   AND TABLE_NAME = :tbl_name
                 ORDER BY COLUMN_ID
                """
            )
            params = {"schema_name": schema_name, "tbl_name": tbl_name}
        else:
            q = text(
                """
                SELECT COLUMN_NAME, DATA_TYPE
                  FROM USER_TAB_COLUMNS
                 WHERE TABLE_NAME = :tbl_name
                 ORDER BY COLUMN_ID
                """
            )
            params = {"tbl_name": tbl_name}

        with eng.connect() as c:
            return [(r[0].upper(), r[1].lower()) for r in c.execute(q, params)]

    def generate_select_safe_cast(
        self,
        schema: List[Tuple[str, str]],
        table: str,
        partition_col: Optional[str] = None,
    ) -> str:
        """
        Gera o SELECT com conversão segura para BigQuery (CAST col AS STRING, INT64, etc).

        A query é pensada para ser usada no Dataform/BigQuery, assim como as demais
        implementações de loaders.
        """
        clauses = []

        if partition_col:
            clauses.append(f"CAST({partition_col} AS DATE) AS DT")

        for col, typ in schema:
            # Evita duplicar DT quando já estamos gerando DT a partir da coluna de partição.
            if partition_col and col.upper() == "DT":
                continue

            bq_type = ORACLE_TYPE_MAP.get(typ, "STRING")
            clauses.append(f"CAST({col} AS {bq_type}) AS {col}")

        # Mantém as colunas técnicas no mesmo padrão usado nos outros loaders
        clauses.append("DT_INGESTAO")
        clauses.append("ID_EXECUCAO")

        indent = "  "
        sep = f",\n{indent}"
        return f"SELECT\n{indent}{sep.join(clauses)}\nFROM ${{ref('{table}')}}"

