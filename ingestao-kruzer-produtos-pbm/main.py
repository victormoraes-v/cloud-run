"""
Ingestão PBM → GCS (Parquet, overwrite)
---------------------------------------
Lê dados de uma API HTTP, aplica uma transformação mínima e grava um arquivo
Parquet em um path fixo no Cloud Storage, SEM histórico (sempre sobrescreve).

Boas práticas adotadas:
- Tipagem estática (type hints) para legibilidade/manutenção.
- Docstrings padronizadas (Args/Returns/Raises) em todas as funções.
- Validação explícita de variáveis de ambiente obrigatórias.
- Logging simples e objetivo (INFO/WARN), timestamps e níveis.
- Timestamps em UTC para auditoria consistente.
- Dependências mínimas.
"""

from __future__ import annotations

import os
import uuid
import logging
from datetime import datetime, timezone
from typing import Any

import requests
import pandas as pd
import pyarrow as pa

import google.cloud.logging

from utils import transform_pbms, write_dataframe_to_gcs

from dotenv import load_dotenv
load_dotenv()

# --- Configuração do Logging (Executa uma vez na inicialização da instância) ---

# Instancia o cliente do Cloud Logging
logging_client = google.cloud.logging.Client()

# Configura o handler para se integrar com o Python logging padrão.
# Isso fará com que todos os logs (logging.info, logging.error, etc.)
# sejam formatados como JSON e enviados para o Cloud Logging.
logging_client.setup_logging()

# --- Fim da Configuração do Logging ---

PROJECT_ID = os.environ.get('GCP_PROJECT')
BUCKET_NAME = os.environ.get('BUCKET_NAME')
API_URL = os.environ.get("API_URL")
FILE_NAME = os.environ.get('FILE_NAME')


def fetch_json(api_url: str) -> Any:
    """
    Realiza requisição HTTP GET e retorna o payload JSON da API.

    Args:
        api_url: URL completa do endpoint a ser consultado.

    Returns:
        Objeto Python equivalente ao JSON retornado (dict/list etc).

    Raises:
        requests.HTTPError: Se a resposta não for 2xx.
        requests.RequestException: Para outros erros de rede/timeout.
        ValueError: Se o corpo não puder ser decodificado como JSON.
    """
    resp = requests.get(api_url, timeout=120)
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    """
    Ponto de entrada do Job: lê ENV, busca API, transforma e grava Parquet.

    Variáveis de ambiente esperadas:
        - API_URL: Endpoint HTTP a ser consultado.
        - BUCKET_NAME: Bucket GCS de destino.
        - FILE_NAME: Nome base do arquivo (sem extensão).

    Fluxo:
        1) Setup de logs.
        2) GET simples na API e decodificação de JSON.
        3) Transformação para DataFrame conforme regras do domínio.
        4) Escrita Parquet (overwrite) no GCS com schema explícito.
    """

    api_url = API_URL
    bucket_name = BUCKET_NAME
    file_name = FILE_NAME

    if not api_url or not bucket_name or not file_name:
        raise RuntimeError("API_URL, BUCKET_NAME e FILE_NAME devem estar definidos no ambiente.")

    logging.info(f"Iniciando ingestão PBM: url={api_url}")
    payload = fetch_json(api_url)

    df = transform_pbms(payload)
    logging.info("Registros após transformação: %s", f"{len(df):,}")

    if df.empty:
        logging.warning("Nenhum dado retornado. Nada a gravar.")
        return

    uri = write_dataframe_to_gcs(df, file_name, bucket_name)
    logging.info("Arquivo sobrescrito em: %s", uri)
    logging.info("Concluído com sucesso.")


if __name__ == "__main__":
    try:
        main()
    finally:
        # força flush e fecha a conexão com Cloud Logging
        logging_client.close()

