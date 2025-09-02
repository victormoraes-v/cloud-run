import os
import io
import logging
import pandas as pd
import smbclient
from google.cloud import secretmanager
from smbprotocol.exceptions import SMBException, SMBAuthenticationError

import os
import io
import logging
import base64
from pathlib import Path

# --- Imports para Logging e Clientes Google ---
import google.cloud.logging
from google.cloud import storage, secretmanager

# --- Importa as funções de tratamento dos arquivos
from processors import get_processor

# --- Imports para utilizar variaveis de ambiente em testes locais
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

def _get_secret(secret_id, version_id="latest"):
    """
    Busca o valor de um secret no Google Cloud Secret Manager.
    """
    # Constrói o nome completo do recurso para o secret
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    
    # Faz a chamada para a API para acessar a versão do secret
    response = secret_client.access_secret_version(request={"name": name})
    
    # O payload do secret vem em bytes, então precisamos decodificá-lo para uma string
    return response.payload.data.decode("UTF-8")


storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()

PROJECT_ID = os.environ.get('GCP_PROJECT')
PROCESSED_BUCKET_NAME = os.environ.get('PROCESSED_BUCKET')
SERVER_IP = '10.0.1.100'
SHARE_PATH = 'Arquivos Suporte PBI'
SMB_USER = _get_secret('admin-bi-user')
SMB_PASSWORD = _get_secret('admin-bi-password')


def main(request):
    """
    Função HTTP com logging detalhado para processar arquivos on-premises.
    """
    logging.info("Função acionada.", extra={"json_fields": {"service_name": "leitor-onprem-service"}})

    request_json = request.get_json(silent=True)

    if not request_json or 'filename' not in request_json:
        # Log de erro claro quando a requisição é malformada
        logging.error("Requisição inválida: o corpo JSON deve conter a chave 'filename'.")
        return ("Requisição inválida.", 400)
    
    file_to_process = request_json['filename']
    unc_path = f"\\\\{SERVER_IP}\\{SHARE_PATH}\\{file_to_process}"
    
    # Log com campos estruturados para facilitar a busca
    log_context = {"filename": file_to_process, "unc_path": unc_path}
    logging.info("Iniciando processamento de arquivo.", extra={"json_fields": log_context})

    try:
        # Etapa 1: Ler o arquivo do compartilhamento de rede
        logging.info("Etapa 1: Acessando compartilhamento de rede (SMB)...")
        with smbclient.open_file(unc_path, mode='rb', username=SMB_USER, password=SMB_PASSWORD) as f:
            file_bytes = f.read()
        logging.info("Arquivo lido da rede com sucesso.", extra={"json_fields": {"file_size_bytes": len(file_bytes)}})

        # Etapa 2: Obter o processador correto usando a fábrica
        logging.info("Etapa 2: Selecionando processador de dados...")
        processor_function = get_processor(file_to_process)
        
        # Etapa 3: Executar a transformação dos dados
        logging.info(f"Etapa 3: Executando processador '{processor_function.__module__}'...")
        df = processor_function(file_bytes)
        logging.info("Dados transformados em DataFrame com sucesso.", extra={"json_fields": {"dataframe_shape": df.shape}})

        # Etapa 4: Salvar o DataFrame como CSV no GCS
        logging.info("Etapa 4: Escrevendo arquivo CSV no Cloud Storage...")
        destination_path = _write_dataframe_to_gcs(df, file_to_process, PROCESSED_BUCKET_NAME)

        # Log de sucesso final com todos os detalhes importantes
        success_log = {
            "source_file": file_to_process,
            "gcs_destination": destination_path,
            "status": "SUCESSO"
        }
        logging.info("Processamento concluído com sucesso.", extra={"json_fields": success_log})
        return ("Processamento concluído com sucesso.", 200)

    # Erro específico para quando o usuário/senha estão errados.
    except SMBAuthenticationError:
        logging.error(f"Falha de autenticação ao conectar em {SERVER_IP}. Verifique o usuário/senha no Secret Manager.", exc_info=False, extra={"json_fields": log_context})
        # Retorna um erro 500 (Erro Interno do Servidor), pois é um problema de configuração
        return (f"Erro de configuração: Falha de autenticação no servidor de arquivos.", 500)

    # Erro específico para quando o arquivo não é encontrado no compartilhamento
    except FileNotFoundError:
        logging.error(f"Arquivo não encontrado no servidor: {unc_path}", exc_info=False, extra={"json_fields": log_context})
        # Retorna um erro 404 (Não Encontrado), que é semanticamente correto
        return (f"Arquivo não encontrado: {file_to_process}", 404)
        
    # Captura outras exceções relacionadas ao protocolo SMB
    except SMBException as e:
        logging.error(f"Ocorreu um erro de protocolo SMB ao acessar {unc_path}. Detalhes: {e}", exc_info=True, extra={"json_fields": log_context})
        return (f"Erro de comunicação com o servidor de arquivos: {e}", 500)

    # Captura genérica para qualquer outro erro inesperado (ex: erro no Pandas)
    except Exception as e:
        logging.critical(f"Falha crítica e inesperada no processamento do arquivo {file_to_process}.", exc_info=True, extra={"json_fields": log_context})
        return (f"Erro interno inesperado no processamento do arquivo: {e}", 500)


def _write_dataframe_to_gcs(df, original_file_name, destination_bucket_name):
    """Converte um DataFrame para CSV e o salva em um bucket do GCS."""
    folder_path = "arquivos/"
    destination_file_name = f"{folder_path}{Path(original_file_name).stem}.csv"
    destination_bucket = storage_client.bucket(destination_bucket_name)
    destination_blob = destination_bucket.blob(destination_file_name)
    
    csv_data = df.to_csv(sep=',', index=False, encoding='utf-8')
    
    destination_blob.upload_from_string(csv_data, content_type='text/csv')
    
    full_path = f"gs://{destination_bucket_name}/{destination_file_name}"
    logging.info(f"DataFrame salvo com sucesso em: {full_path}")
    return full_path


# SERVER_IP = '10.0.1.100'
# SHARE_PATH = 'Arquivos Suporte PBI'

# file_path = f"\\\\{SERVER_IP}\\{SHARE_PATH}\\Info Lojas Servicos Farmaceuticos.xlsx"

# df = pd.read_excel(file_path, header=4)
