import os
import sys
import logging
from dotenv import load_dotenv
import json

from processors import process_generic
from utils import get_secret, get_all_migration_table_configs

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
        migration_config = json.loads(get_secret(PROJECT_ID, "arquivos_rede_pipeline_migration_config"))
        conn_config_secret = migration_config['data_connection_config_file_name']
        table_config_name = migration_config['ingestion_config_table']

        creds = json.loads(get_secret(PROJECT_ID, conn_config_secret))['connections'][0]
        server_ip = creds['database_hostname']
        smb_user = creds['database_username']
        smb_password = creds['database_password']

        # 1) Busca TODAS as configs do arquivo (pode ter múltiplas abas)
        configs = get_all_migration_table_configs(table_config_name, file_to_process)

        logging.info(f"🔎 Encontradas {len(configs)} configuração(ões) para o arquivo {file_to_process}")

        # 2) Monta o caminho smb (usa a primeira config para pegar o file_path)
        file_path = f"\\\\{server_ip}\\{configs[0]['file_path']}\\{file_to_process}"
        log_ctx = {"filename": file_to_process, "file_path": file_path, "total_configs": len(configs)}

        # 3) Processa cada configuração (cada aba)
        processed_count = 0
        for idx, cfg in enumerate(configs, 1):
            sheet_name = cfg.get('sheet_name', 'N/A')
            logging.info(
                f"📄 Processando configuração {idx}/{len(configs)} - Aba: {sheet_name}",
                extra={"json_fields": {**log_ctx, "sheet_name": sheet_name, "config_index": idx}}
            )

            try:
                process_generic(
                    cfg=cfg,
                    file_path=file_path,
                    username=smb_user,
                    password=smb_password,
                    bucket_name=PROCESSED_BUCKET_NAME
                )
                processed_count += 1
                logging.info(
                    f"✅ Configuração {idx}/{len(configs)} processada com sucesso (Aba: {sheet_name})",
                    extra={"json_fields": {**log_ctx, "sheet_name": sheet_name}}
                )
            except Exception as e:
                logging.error(
                    f"❌ Erro ao processar configuração {idx}/{len(configs)} (Aba: {sheet_name}): {e}",
                    exc_info=True,
                    extra={"json_fields": {**log_ctx, "sheet_name": sheet_name}}
                )
                # Continua processando as outras configurações mesmo se uma falhar
                continue

        logging.info(
            f"Processamento concluído: {processed_count}/{len(configs)} configuração(ões) processada(s) com sucesso.",
            extra={"json_fields": {**log_ctx, "processed_count": processed_count}}
        )
        return 0

    except Exception as e:
        log_ctx = {"filename": file_to_process}
        logging.critical(f"Falha no processamento: {e}", exc_info=True, extra={"json_fields": log_ctx})
        return 1


if __name__ == "__main__":
    sys.exit(run_job())
