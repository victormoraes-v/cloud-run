import pandas as pd
import logging
from src.utils.normalization import normalize_competitor

def add_new_column(df):
    """
    Essa função executa um tratamento muito simples, normalizando as colunas de concorrentes,
    e escolhendo a melhor versão entre elas, para formar a coluna "CONCORRENTE"
    """
    # Normalização do concorrente
    logging.info("Normalizando nomes de concorrentes...")
    df['CONCORRENTE'] = df.apply(lambda row: normalize_competitor(row, 'DOMAIN', 'SOLD_BY', 'SELLERS'), axis=1)

    return df
 