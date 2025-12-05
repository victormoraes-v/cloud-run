import os
import sys
import logging
from dotenv import load_dotenv
import json

from processors import process_generic
from utils import get_secret, build_destination_path, get_migration_table_config

load_dotenv()
logging.basicConfig(level=logging.INFO)

# --- Variáveis de ambiente ---
PROJECT_ID = os.environ.get("GCP_PROJECT")
PROCESSED_BUCKET_NAME = os.environ.get("PROCESSED_BUCKET")

def run_job():
    file_to_process = os.environ.get("FILE_TO_PROCESS")
    if not file_to_process:
        raise ValueError("É obrigatório definir a variável de ambiente FILE_TO_PROCESS")

    try:
        # 0) Busca secrets
        creds = json.loads(get_secret(PROJECT_ID, "arquivos_rede_pipeline_migration_config"))
        server_ip = creds['host']
        smb_user = creds['username']
        smb_password = creds['password']
        table_config_name = creds['migration_config_table']

        # 1) Busca config do arquivo
        row = get_migration_table_config(table_config_name, file_to_process)

        # 2) Converte Row para dict
        cfg = dict(row)

        # log do que achou
        logging.info(f"🔎 Config carregada: {cfg}")

        # 3) Monta o caminho smb
        file_path = f"\\\\{server_ip}\\{cfg['file_path']}\\{file_to_process}"
        log_ctx = {"filename": file_to_process, "file_path": file_path}

        # 5) Executa processamento
        logging.info(f"Iniciando processor para {file_to_process}", extra={"json_fields": log_ctx})

        process_generic(
            cfg=cfg,
            file_path=file_path,
            username=smb_user,
            password=smb_password,
            bucket_name=PROCESSED_BUCKET_NAME
        )

        logging.info("Processamento concluído com sucesso.", extra={"json_fields": log_ctx})
        return 0

    except Exception as e:
        log_ctx = {"filename": file_to_process}
        logging.critical(f"Falha no processamento: {e}", exc_info=True, extra={"json_fields": log_ctx})
        return 1


if __name__ == "__main__":
    sys.exit(run_job())
