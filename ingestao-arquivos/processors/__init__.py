import logging
from . import arvore_mkt #... e outros
#from . import default_processor # Importante ter um fallback

# Dicionário mapeando a chave ao módulo de processamento
PROCESSOR_MAP = {
    'Árvore MKT - Servicos': arvore_mkt
}

def get_processor(filename):
    """Encontra e retorna a função de processamento correta."""
    for key, processor_module in PROCESSOR_MAP.items():
        if key in filename:
            logging.info(f"Processador específico '{processor_module.__name__}' encontrado para o arquivo '{filename}'.")
            return processor_module.process
    
    # Se o loop terminar sem encontrar, use o padrão
    logging.warning(
        f"Nenhum processador específico encontrado para '{filename}'. Usando o processador padrão.",
        extra={"json_fields": {"filename": filename}}
    )
