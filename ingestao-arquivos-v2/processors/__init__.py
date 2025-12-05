"""
Pacote de processadores de arquivos.

- Contém o processador genérico
- Processadores customizados são carregados dinamicamente (importlib)
"""

from .processor_generic_smb import process_generic

__all__ = [
    "process_generic"
]
