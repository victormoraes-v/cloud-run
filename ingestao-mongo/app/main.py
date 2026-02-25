from datetime import datetime, timezone
from dotenv import load_dotenv
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

def run():
    logger = setup_logging()
    logger.info("🚀 Iniciando job Mongo → GCS (RAW ingest)")

    project_id = os.getenv("GCP_PROJECT")
    config_table = os.getenv("CONFIG_SECRET_NAME")
    pipeline_name = os.getenv("PIPELINE_NAME")
    start_date = os.getenv("START_DATE")
    env = os.getenv("ENV", "PROD")

    run_ts = datetime.now(timezone.utc)
    run_id = run_ts.strftime("%Y%m%dT%H%M%SZ")
    logger.info("🧾 run_id=%s dt_ingestao=%s", run_id, run_ts.isoformat())

    mongo_config_secret = load_mongo_secret(config_table)
    db_secret = mongo_config_secret['data_connection_config_file_name']
    migration_config_dataset = mongo_config_secret['migration_config_dataset']
    migration_config_table = mongo_config_secret['migration_config_table']

    # 1) Buscar config no catálogo BigQuery
    bq = bigquery.Client()
    sql = f"""
        SELECT * FROM `{project_id}.{migration_config_dataset}.{migration_config_table}`
        WHERE collection_name = @name AND active = TRUE
    """
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("name", "STRING", pipeline_name)]
    ))
    row = list(job)[0]

    # 2) Ler secret do Mongo
    mongo_secret = load_mongo_secret(db_secret)['connections'][0]

    # 3) Conectar
    repo = MongoRepository(
        mongo_secret=mongo_secret,
        collection_name=row.COLLECTION_NAME
    )

    # 4) Executar STANDARD ou FREE
    if row.PIPELINE_TYPE == "STANDARD":
        logger.info("🔍 Execução em modo STANDARD")

        # A tabela de configuração deve ter as colunas TARGET_DATASET e TARGET_TABLE
        logger.info(f"Buscando data de corte em {project_id}.{row.TARGET_DATASET}.{row.TARGET_TABLE_NAME}")
        incremental_ts = start_date or get_max_date_from_bq_table(
            bq=bq,
            project_id=project_id,
            target_dataset=row.TARGET_DATASET,
            target_table=row.TARGET_TABLE_NAME
        )
        logger.info(f"Data de corte: {incremental_ts}")

        query = build_mongo_query(row.FILTER_COLUMN, incremental_ts)
        projection = build_projection(json.loads(row.PROJECTION))
        cursor = repo.find(query, projection, no_cursor_timeout=True, batch_size=20000)

    elif row.PIPELINE_TYPE == "FREE":
        logger.info("🔍 Execução em modo FREE (aggregation pipeline)")
        pipeline = json.loads(row.MONGO_QUERY)
        cursor = repo.aggregate(pipeline, batchSize=20000)

    else:
        raise ValueError(f"pipeline_type inválido: {row.PIPELINE_TYPE}")

    chunk_size = 20000
    total_docs = 0
    chunk_count = 0

    prefix = f'mongo/{mongo_secret["database_name"]}/{row.COLLECTION_NAME}'
    if env == 'TEST':
        prefix = f'mongo_test/{mongo_secret["database_name"]}/{row.COLLECTION_NAME}'

    for i, batch in enumerate(chunked_cursor(cursor, chunk_size)):
        chunk_count += 1
        total_docs += len(batch)

        logger.info("📦 [%s] Chunk %s — docs=%s", row.COLLECTION_NAME, i, len(batch))

        df = normalize_documents(batch)

        dataframe_to_parquet_gcs(
            df=df,
            bucket_name=os.getenv("BUCKET_NAME"),
            prefix=prefix,
            file_prefix=f"{row.COLLECTION_NAME}_part_{i:05d}",
            ingest_ts=run_ts,
            run_id=run_id
        )

    if total_docs == 0:
        logger.warning("⚠ [%s] Nenhum dado. Encerrando.", row.COLLECTION_NAME)
        return

    logger.info("✅ [%s] Finalizado — total_docs=%s chunks=%s", row.COLLECTION_NAME, total_docs, chunk_count)


if __name__ == "__main__":
    run()