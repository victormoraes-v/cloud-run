# db/base_loader.py
"""
Classe base abstrata (ABC) para padronizar loaders de bancos relacionais.
Qualquer banco (SQL Server, PostgreSQL, etc.) deve implementar os mesmos métodos.
"""

from abc import ABC, abstractmethod
from sqlalchemy import engine
from typing import List, Tuple, Optional


class BaseLoader(ABC):
    """
    Define o contrato que todos os loaders devem seguir.
    A ideia é padronizar a interface, garantindo que qualquer novo banco implementado
    funcione automaticamente sem mudar outras partes do sistema.
    """

    @abstractmethod
    def get_engine(self, conn_json: str) -> engine.Engine:
        """
        Cria e retorna a engine SQLAlchemy para o banco em questão.
        """
        pass

    @abstractmethod
    def get_schema(self, eng: engine.Engine, table: str) -> List[Tuple[str, str]]:
        """
        Retorna uma lista com o schema da tabela no formato:
        [(NOME_DA_COLUNA, TIPO_DO_BANCO)]
        """
        pass

    @abstractmethod
    def generate_select_safe_cast(
        self,
        schema: List[Tuple[str, str]],
        table: str,
        partition_col: Optional[str] = None
    ) -> str:
        """
        Gera uma query SELECT contendo SAFE_CAST para cada coluna,
        convertendo os tipos nativos do banco para tipos compatíveis com BigQuery.
        """
        pass
