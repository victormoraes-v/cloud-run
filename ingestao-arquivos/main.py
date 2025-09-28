import os
import sys
import logging
from dotenv import load_dotenv

from processors import get_processor
from utils import get_secret, build_destination_path

load_dotenv()
logging.basicConfig(level=logging.INFO)

# --- Variáveis de ambiente ---
PROJECT_ID = os.environ.get("GCP_PROJECT")
PROCESSED_BUCKET_NAME = os.environ.get("PROCESSED_BUCKET")
SERVER_IP = os.environ.get("SMB_SERVER_IP", "10.0.1.100")
SHARE_PATH = os.environ.get("SMB_SHARE_PATH", "Arquivos Suporte PBI")

SMB_USER = get_secret(PROJECT_ID, "admin-bi-user")
SMB_PASSWORD = get_secret(PROJECT_ID, "admin-bi-password")


def run_job():
    file_to_process = os.environ.get("FILE_TO_PROCESS")
    if not file_to_process:
        raise ValueError("É obrigatório definir a variável de ambiente FILE_TO_PROCESS")

    file_path = f"\\\\{SERVER_IP}\\{SHARE_PATH}\\{file_to_process}"
    log_ctx = {"filename": file_to_process, "file_path": file_path}

    try:
        processor_function, file_format, write_mode = get_processor(file_to_process)
        if not processor_function:
            raise ValueError(f"Nenhum processador configurado para '{file_to_process}'")

        logging.info(f"Iniciando processor para {file_to_process}", extra={"json_fields": log_ctx})

        destination_path = build_destination_path(file_to_process, "arquivos/", write_mode, file_format)
        # O processor faz tudo: leitura, transformação e escrita
        processor_function(
            file_path=file_path,
            username=SMB_USER,
            password=SMB_PASSWORD,
            bucket_name=PROCESSED_BUCKET_NAME,
            file_to_process=file_to_process,
            file_format=file_format,
            write_mode=write_mode,
        )

        logging.info("Processamento concluído com sucesso.", extra={"json_fields": log_ctx})
        return 0

    except Exception as e:
        logging.critical(f"Falha no processamento: {e}", exc_info=True, extra={"json_fields": log_ctx})
        return 1


if __name__ == "__main__":
    sys.exit(run_job())
