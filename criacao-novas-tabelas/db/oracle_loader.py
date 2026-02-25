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
        Lê o schema Oracle de forma robusta:

        Fluxo:
        1) tenta ALL_TAB_COLUMNS direto (schema informado ou default)
        2) se vazio → resolve via ALL_SYNONYMS
        3) reconsulta ALL_TAB_COLUMNS com owner real
        4) valida retorno

        Funciona para:
        - Views em APPS
        - Tabelas custom
        - Tabelas padrão EBS via synonym (AR, HZ, etc)
        """

        parts = table.split(".")
        input_schema = parts[0].upper() if len(parts) > 1 else None
        tbl_name = parts[1].upper() if len(parts) > 1 else parts[0].upper()

        def fetch_columns(owner):
            owner_clause = "AND OWNER = :owner" if owner else ""
            sql = text(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM ALL_TAB_COLUMNS
                WHERE TABLE_NAME = :tbl_name
                {owner_clause}
                ORDER BY COLUMN_ID
            """)

            params = {"tbl_name": tbl_name}
            if owner:
                params["owner"] = owner

            with eng.connect() as c:
                rows = c.execute(sql, params).fetchall()

            return [(r[0].upper(), r[1].lower()) for r in rows]

        # ----------------------------------------------------
        # 1️⃣ Tentativa direta (view ou tabela real no schema informado)
        # ----------------------------------------------------
        schema = fetch_columns(input_schema)

        if schema:
            return schema

        # ----------------------------------------------------
        # 2️⃣ Resolver synonym
        # ----------------------------------------------------
        synonym_sql = text("""
            SELECT TABLE_OWNER
            FROM ALL_SYNONYMS
            WHERE SYNONYM_NAME = :tbl_name
            ORDER BY DECODE(OWNER,'PUBLIC',2,1)
            FETCH FIRST 1 ROWS ONLY
        """)

        with eng.connect() as c:
            r = c.execute(synonym_sql, {"tbl_name": tbl_name}).fetchone()

        if not r:
            raise RuntimeError(f"Não foi possível resolver synonym para {table}")

        real_owner = r[0]

        # ----------------------------------------------------
        # 3️⃣ Rebuscar colunas com owner real
        # ----------------------------------------------------
        schema = fetch_columns(real_owner)

        if not schema:
            raise RuntimeError(f"Tabela {real_owner}.{tbl_name} encontrada mas sem colunas!")

        return schema


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

