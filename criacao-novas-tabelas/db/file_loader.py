# db/file_loader.py
"""
Loader para arquivos CSV/Excel/TXT/Parquet.
Serve como substituto dos loaders de bancos.
Implementa a mesma interface dos outros loaders para compatibilidade.
"""
import pandas as pd
from typing import List, Tuple, Optional
import os

class FileLoader:
    """
    Loader para arquivos CSV/Excel/TXT/Parquet.
    Serve como substituto dos loaders de bancos.
    Implementa métodos compatíveis com a interface dos loaders de banco.
    """

    def __init__(self, file_path: str = None, read_params: dict = None):
        self.file_path = file_path
        self.read_params = read_params or {}

    def get_engine(self, conn_json: str = None):
        """
        Para arquivos, não precisamos de engine SQLAlchemy.
        Retorna None para manter compatibilidade com a interface.
        """
        # Se conn_json contém informações do arquivo, atualiza
        if conn_json:
            import json
            try:
                config = json.loads(conn_json)
                if 'file_path' in config:
                    self.file_path = config['file_path']
                if 'read_params' in config:
                    self.read_params.update(config['read_params'])
            except:
                pass
        return None  # Arquivos não usam engine

    def get_schema(self, eng=None, table: str = None) -> List[Tuple[str, str]]:
        """
        (NO-OP PARA ARQUIVOS)

        Antes: lia o arquivo e inferia tipos pandas → BigQuery.
        Agora: não faz mais nada para não quebrar o fluxo de execução.

        Mantido apenas para compatibilidade de interface com os demais loaders.

        Returns:
            Lista vazia de colunas/tipos, já que o SELECT de arquivos usa sempre
            SELECT * ou a lógica com partição baseada na coluna de configuração.
        """
        return []

    def _pandas_to_bq(self, dtype) -> str:
        """Converte tipos do pandas para tipos do BigQuery."""
        dtype_str = str(dtype).lower()
        if "int" in dtype_str:
            return "INT64"
        if "float" in dtype_str:
            return "FLOAT64"
        if "datetime" in dtype_str or "timestamp" in dtype_str:
            return "DATETIME"
        if "bool" in dtype_str:
            return "BOOL"
        return "STRING"

    def generate_select_safe_cast(
        self, 
        schema: List[Tuple[str, str]], 
        table: str, 
        partition_col: Optional[str] = None
    ) -> str:
        """
        Gera o SELECT para arquivos.
        
        Regras:
        - Sempre faz SELECT * FROM para arquivos sem partição.
        - Se houver coluna de partição, gera:
            SELECT
              CAST(PARTITION_COLUMN AS DATE) AS DT,
              *,
              EXCEPT(DT)
            FROM ${ref('tabela')}
        """
        indent = "  "

        # Sem partição → SELECT * FROM
        if not partition_col:
            return f"SELECT\n{indent}*\nFROM ${{ref('{table}')}}"

        # Com partição → SELECT CAST(PARTITION_COLUMN AS DATE) AS DT, *, EXCEPT(DT)
        lines = [
            "SELECT",
            f"{indent}CAST({partition_col} AS DATE) AS DT,",
            f"{indent}*,",
            f"{indent}EXCEPT(DT)",
            f"FROM ${{ref('{table}')}}",
        ]
        return "\n".join(lines)
