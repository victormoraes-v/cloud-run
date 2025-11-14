import logging
import os
import sys
import json
from src.config.loader import load_config
from src.core.api_client import PrecificaAPIClient
from src.processing.precifica_parser import process_api_results
from src.processing.transform import add_new_column
from src.storage.gcs import save_df_to_gcs_csv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def run_job():
    try:
        cfg = load_config()
        
        # Log de diagnóstico
        logging.info("=== Diagnóstico de Configuração ===")
        base_url = cfg.get("API", "BASE_URL", fallback="")
        logging.info(f"API_BASE_URL: {base_url}")
        logging.info(f"API_CLIENT_KEY: {'***' if cfg.get('API', 'CLIENT_KEY', fallback='') else 'VAZIO'}")
        logging.info(f"API_SECRET_KEY: {'***' if cfg.get('API', 'SECRET_KEY', fallback='') else 'VAZIO'}")
        logging.info(f"API_PLATAFORMA: {cfg.get('API', 'PLATAFORMA', fallback='VAZIO')}")
        logging.info(f"API_DOMINIO: {cfg.get('API', 'DOMINIO', fallback='VAZIO')}")
        
        if not base_url:
            raise ValueError("API_BASE_URL não está configurado! Verifique o secret 'precifica-extracao-config'")
        
        bucket = cfg.get("GCS", "BUCKET")
        prefix = cfg.get("GCS", "PREFIX", fallback="raw/precifica/")

        client = PrecificaAPIClient(cfg)
        all_products = client.fetch_all_products_concurrently(max_workers=8)
        df = process_api_results(all_products)

        df = add_new_column(df)

        if df.empty:
            result = {"saved": False, "rows": 0}
            logging.info(json.dumps(result))
            return result

        object_name = save_df_to_gcs_csv(df, bucket, prefix)

        result = {"saved": True, "rows": len(df), "gcs_object": object_name}
        logging.info(json.dumps(result))
        return result
    except Exception as e:
        #result = {"saved": False, "rows": 0}
        logging.exception(f"error: {str(e)}")

if __name__ == "__main__":
    run_job()