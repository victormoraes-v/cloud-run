import pandas as pd
import io
import logging

def process(file_bytes):
    """Processa o arquivo 'Info Lojas Servicos Farmaceuticos.xlsx'."""
    logging.info(f"Dentro do processador '{__name__}': aplicando regras específicas.")
    
    df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl', header=4)
    logging.info("Arquivo Excel lido com sucesso.")
        
    return df