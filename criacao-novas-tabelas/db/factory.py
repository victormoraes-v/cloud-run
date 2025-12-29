# db/factory.py
"""
Factory que retorna automaticamente o loader correto de acordo com o tipo da fonte de dados.
Suporta bancos de dados (SQL Server, PostgreSQL) e arquivos (CSV, Excel, Parquet).
Evita espalhar IF de tipo pelo projeto inteiro.
"""

from .sql_server_loader import SqlServerLoader
from .postgres_loader import PostgresLoader
from .file_loader import FileLoader

def get_loader(data_source_type: str, file_path: str = None, read_params: dict = None):
    """
    Retorna a instância do loader apropriado.
    O restante do sistema nunca precisa saber qual fonte está sendo usada.
    
    Args:
        data_source_type: Tipo da fonte de dados ('SQL_SERVER', 'POSTGRES', 'FILE', etc.)
        file_path: Caminho do arquivo (necessário apenas para FILE)
        read_params: Parâmetros de leitura do arquivo (necessário apenas para FILE)
    
    Returns:
        Instância do loader apropriado
    """
    data_source_type = data_source_type.upper()

    if data_source_type in ("SQL_SERVER", "MSSQL", "MSSQL_SERVER"):
        return SqlServerLoader()

    if data_source_type in ("POSTGRES", "POSTGRESQL", "PG"):
        return PostgresLoader()

    if data_source_type in ("FILES", "CSV", "EXCEL", "PARQUET", "TXT"):
        return FileLoader(file_path=file_path, read_params=read_params or {})

    raise ValueError(f"Tipo de fonte de dados '{data_source_type}' não suportado.")
