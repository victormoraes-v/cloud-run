# db/factory.py
"""
Factory que retorna automaticamente o loader correto de acordo com o tipo do banco.
Evita espalhar IF de banco pelo projeto inteiro.
"""

from .sql_server_loader import SqlServerLoader
from .postgres_loader import PostgresLoader

def get_loader(db_type: str):
    """
    Retorna a instância do loader apropriado.
    O restante do sistema nunca precisa saber qual banco está sendo usado.
    """
    db_type = db_type.upper()

    if db_type in ("SQL_SERVER", "MSSQL", "MSSQL_SERVER"):
        return SqlServerLoader()

    if db_type in ("POSTGRES", "POSTGRESQL", "PG"):
        return PostgresLoader()

    raise ValueError(f"Tipo de banco '{db_type}' não suportado.")
