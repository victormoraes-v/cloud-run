import logging
from . import (
    arvore_mkt, 
    info_lojas, 
    dark_stores, 
    info_lojas_servicos_farmaceuticos,
    produtos_pricepoint
)
#from . import default_processor # Importante ter um fallback

# Dicionário mapeando a chave ao módulo de processamento
PROCESSOR_MAP = {
    'Árvore MKT - Servicos.xlsx': arvore_mkt,
    'Info Lojas.xlsx': info_lojas,
    "Darks_Store_Lojas.xlsx": dark_stores,
    "Info Lojas Servicos Farmaceuticos.xlsx": info_lojas_servicos_farmaceuticos,
    "PRICEPOINT_CADASTRO PRODUTO_v2.xlsx": produtos_pricepoint
}

def get_processor(filename):
    """Encontra e retorna a função de processamento correta."""
    processor_module = PROCESSOR_MAP.get(filename)
    logging.info(f"Processador específico '{processor_module.__name__}' encontrado para o arquivo '{filename}'.")
    if processor_module:
        return processor_module.process
    # for key, processor_module in PROCESSOR_MAP.items():
    #     if key in filename:
    #         logging.info(f"Processador específico '{processor_module.__name__}' encontrado para o arquivo '{filename}'.")
    #         return processor_module.process
    
    # Se o loop terminar sem encontrar, use o padrão
    logging.warning(
        f"Nenhum processador específico encontrado para '{filename}'.",
        extra={"json_fields": {"filename": filename}}
    )
