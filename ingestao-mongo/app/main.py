from datetime import datetime, timezone
from dotenv import load_dotenv

from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery

from .logging_config import setup_logging
from .config.config import load_mongo_secret
from .mongo_client import MongoRepository
from .services.extractor import build_mongo_query, build_projection, get_max_date_from_bq_table
from .services.transformer import normalize_documents
from .services.writer import dataframe_to_parquet_gcs
from .services.chunking import chunked_cursor

import os
import json

load_dotenv()

def _process_collection(row, project_id, run_ts, run_id, db_secret):
    logger = setup_logging()
    bq = bigquery.Client()  # cria client por thread (mais seguro)

    # 1) Ler secret do Mongo
    mongo_secret = load_mongo_secret(db_secret)['connections'][0]

    # 2) Conectar no Mongo (client por thread)
    repo = MongoRepository(
        mongo_secret=mongo_secret,
        collection_name=row.COLLECTION_NAME
    )

    # 3) Buscar dados (STANDARD/FREE)
    if row.PIPELINE_TYPE == "STANDARD":
        incremental_ts = get_max_date_from_bq_table(
            bq=bq,
            project_id=project_id,
            target_dataset=row.TARGET_DATASET,
            target_table=row.TARGET_TABLE_NAME
        )
        query = build_mongo_query(row.FILTER_COLUMN, incremental_ts)
        
        projection = None

        if row.PROJECTION:
            projection_list = json.loads(row.PROJECTION)
            if projection_list:  # lista não vazia
                projection = build_projection(projection_list)
        
        cursor = repo.find(query, projection, no_cursor_timeout=True, batch_size=20000)

    elif row.PIPELINE_TYPE == "FREE":
        pipeline = json.loads(row.MONGO_QUERY)
        cursor = repo.aggregate(pipeline, batch_size=20000)

    else:
        raise ValueError(f"pipeline_type inválido: {row.PIPELINE_TYPE}")

    # 4) Chunk -> Parquet
    chunk_size = 20000
    total_docs = 0

    prefix = f'mongo/{mongo_secret["database_name"]}/{row.COLLECTION_NAME}'

    for i, batch in enumerate(chunked_cursor(cursor, chunk_size)):
        total_docs += len(batch)

        df = normalize_documents(batch)

        dataframe_to_parquet_gcs(
            df=df,
            bucket_name=os.getenv("BUCKET_NAME"),
            prefix=prefix,
            file_prefix=f"{row.COLLECTION_NAME}_part_{i:05d}",
            ingest_ts=run_ts,
            run_id=run_id
        )

    logger.info("✅ [%s] Finalizado — docs=%s", row.COLLECTION_NAME, total_docs)
    return total_docs


def run():
    logger = setup_logging()
    logger.info("🚀 Iniciando job Mongo → GCS (RAW ingest)")

    project_id = os.getenv("GCP_PROJECT")
    config_table = os.getenv("CONFIG_SECRET_NAME")
    # pipeline_name = os.getenv("PIPELINE_NAME")
    collections_env = os.getenv("COLLECTIONS")
    # start_date = os.getenv("START_DATE")
    # env = os.getenv("ENV", "PROD")

    if collections_env:
        logger.info("📌 Filtrando collections via env: %s", collections_env)
    else:
        logger.info("📌 Processando todas as collections ativas")

    run_ts = datetime.now(timezone.utc)
    run_id = run_ts.strftime("%Y%m%dT%H%M%SZ")
    logger.info("🧾 run_id=%s dt_ingestao=%s", run_id, run_ts.isoformat())

    mongo_config_secret = load_mongo_secret(config_table)
    db_secret = mongo_config_secret['data_connection_config_file_name']
    migration_config_dataset = mongo_config_secret['migration_config_dataset']
    migration_config_table = mongo_config_secret['migration_config_table']

    # 1) Buscar config no catálogo BigQuery
    bq = bigquery.Client()

    base_sql = f"""
        SELECT *
        FROM `{project_id}.{migration_config_dataset}.{migration_config_table}`
        WHERE active = TRUE
    """
    query_parameters = []

    if collections_env:
        collections_list = [c.strip() for c in collections_env.split(",") if c.strip()]
        
        if collections_list:
            base_sql += " AND COLLECTION_NAME IN UNNEST(@collections)"
            
            query_parameters.append(
                bigquery.ArrayQueryParameter(
                    "collections",
                    "STRING",
                    collections_list
                )
            )

    job_config = bigquery.QueryJobConfig(
        query_parameters=query_parameters
    )

    job = bq.query(base_sql, job_config=job_config)
    
    #job = bq.query(sql)
    # job = bq.query(sql, job_config=bigquery.QueryJobConfig(
    #     query_parameters=[bigquery.ScalarQueryParameter("name", "STRING", pipeline_name)]
    # ))

    rows = list(job)
    if not rows:
        logger.warning("⚠ Nenhuma coleção ativa encontrada no catálogo para %s", collections_env)
        return

    max_workers = int(os.getenv("MAX_WORKERS", "3"))
    logger.info("Executando %s coleções em paralelo (max_workers=%s)", len(rows), max_workers)

    results = {}
    errors = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_process_collection, row, project_id, run_ts, run_id, db_secret): row.COLLECTION_NAME
            for row in rows
        }

        for future in as_completed(future_map):
            collection = future_map[future]
            try:
                results[collection] = future.result()
            except Exception:
                errors += 1
                logger.exception("❌ Falha na coleção %s — continuando", collection)

    logger.info("🎯 Execução finalizada. Sucesso=%s Falhas=%s run_id=%s", len(results), errors, run_id)


if __name__ == "__main__":
    run()