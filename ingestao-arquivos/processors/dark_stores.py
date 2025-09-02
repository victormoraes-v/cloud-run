import pandas as pd
import io
import logging

def process(file_bytes):
    """Processa o arquivo 'Darks_Store_Lojas.xlsx'."""
    logging.info(f"Dentro do processador '{__name__}': aplicando regras específicas.")
    
    df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl', sheet_name='Darks')
    logging.info("Arquivo Excel lido com sucesso.")
        
    return df