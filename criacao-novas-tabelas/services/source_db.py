# Arquivo: source_db.py
"""
Módulo para interagir com o banco de dados de origem (SQL Server).
- Obter schema da tabela
- Gerar cláusulas de SELECT com SAFE_CAST
"""
import json
from sqlalchemy import create_engine, engine, text
from typing import Optional
from urllib.parse import quote_plus

# Mapeamento de tipos do SQL Server para o BigQuery
SQL_TO_BQ_TYPE_MAP = {
    'varchar': 'STRING', 'nvarchar': 'STRING', 'char': 'STRING', 'text': 'STRING', 'nchar': 'STRING',
    'ntext': 'STRING', 'xml': 'STRING', 'uniqueidentifier': 'STRING',
    'int': 'INT64', 'bigint': 'INT64', 'smallint': 'INT64', 'tinyint': 'INT64',
    'bit': 'BOOL', 'decimal': 'NUMERIC', 'numeric': 'NUMERIC', 'money': 'NUMERIC', 'smallmoney': 'NUMERIC',
    'float': 'FLOAT64', 'real': 'NUMERIC', 'date': 'DATE', 'datetime': 'DATETIME', 'datetime2': 'DATETIME',
    'smalldatetime': 'DATETIME', 'time': 'TIME', 'datetimeoffset': 'TIMESTAMP', 'binary': 'BYTES',
    'varbinary': 'BYTES', 'image': 'BYTES', 'timestamp': 'INT64'
}

def get_db_engine(db_connection_json: str) -> engine.Engine:
    """
    Cria uma engine SQLAlchemy a partir de uma string JSON de conexão.

    Args:
        db_connection_json: String JSON contendo os detalhes da conexão.

    Returns:
        Uma instância da engine SQLAlchemy.
    """
    db_conn = json.loads(db_connection_json)['connections'][0]
    encoded_password = quote_plus(db_conn['database_password'])
    db_url = (f"mssql+pymssql://{db_conn['database_username']}:{encoded_password}@"
              f"{db_conn['database_hostname']}:{db_conn['database_port']}/{db_conn['database_name']}")
    return create_engine(db_url)

def get_source_table_schema(db_engine: engine.Engine, source_table_name: str) -> list[tuple[str, str]]:
    """
    Obtém o schema (nome da coluna, tipo de dado) de uma tabela no SQL Server.

    Args:
        db_engine: A engine SQLAlchemy para a conexão.
        source_table_name: O nome da tabela de origem (ex: 'dbo.minha_tabela').

    Returns:
        Uma lista de tuplas, onde cada tupla é (COLUMN_NAME, DATA_TYPE).
    """
    parts = source_table_name.split('.')
    schema_name = parts[0] if len(parts) > 1 else 'dbo'
    table_name = parts[1] if len(parts) > 1 else source_table_name

    query = text(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}' ORDER BY ORDINAL_POSITION;"
    )

    with db_engine.connect() as connection:
        result = connection.execute(query).fetchall()
        return [(row[0].upper(), row[1].lower()) for row in result]

def generate_safe_cast_select(
    schema: list[tuple[str, str]],
    source_table_name: str,
    partition_col_to_add: Optional[str] = None
) -> str:
    """
    Gera uma cláusula SELECT formatada com SAFE_CAST para cada coluna, baseada no schema.
    Opcionalmente, adiciona uma coluna de partição como a primeira coluna, convertida para DATE.

    Args:
        schema: O schema da tabela, vindo de get_source_table_schema.
        source_table_name: O nome da tabela de origem, usado no ${{ref(...)}}.
        partition_col_to_add: O nome da coluna a ser adicionada e convertida para DATE.

    Returns:
        Uma string formatada com a cláusula SELECT completa.
    """
    select_clauses = []

    if partition_col_to_add:
        # Garante que a coluna de partição seja a primeira e esteja no formato correto
        partition_clause = f"CAST({partition_col_to_add.upper()} AS DATE) AS DT"
        select_clauses.append(partition_clause)
        print(f"Adicionando coluna de partição '{partition_col_to_add}' como 'dt' no SELECT.")

    for col, sql_type in schema:
        bq_type = SQL_TO_BQ_TYPE_MAP.get(sql_type, 'STRING')
        select_clauses.append(f"CAST({col} AS {bq_type}) AS {col}")

    select_clauses.append("CAST(DT_INGESTAO AS DATETIME) AS DT_INGESTAO")
    select_clauses.append("ID_EXECUCAO")

    if not select_clauses:
        raise ValueError(f"Não foi possível gerar cláusulas de SELECT para a tabela {source_table_name}.")

    indent = "  "
    separator = f",\n{indent}"
    select_part = separator.join(select_clauses)
    return f"SELECT\n{indent}{select_part}\nFROM ${{ref('{source_table_name}')}}"