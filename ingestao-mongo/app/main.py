from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery
from .logging_config import setup_logging
from .config import load_mongo_secret
from .mongo_client import MongoRepository
from .services.extractor import build_mongo_query, build_projection
from .services.transformer import normalize_documents
from .services.writer import dataframe_to_parquet_gcs
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
        ts = start_date or "1900-01-01T00:00:00Z"
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))

        query = build_mongo_query(row.FILTER_COLUMN, ts)
        projection = build_projection(json.loads(row.PROJECTION))
        cursor = repo.find(query, projection)

    elif row.PIPELINE_TYPE == "FREE":
        logger.info("🔍 Execução em modo FREE (aggregation pipeline)")
        pipeline = json.loads(row.MONGO_QUERY)
        cursor = repo.aggregate(pipeline)

    else:
        raise ValueError(f"pipeline_type inválido: {row.PIPELINE_TYPE}")

    # 5) Converter para DataFrame RAW
    documents = list(cursor)
    logger.info(f"📦 Documentos retornados: {len(documents)}")
    if not documents:
        logger.warning("⚠ Nenhum dado. Encerrando.")
        return

    df = normalize_documents(documents)

    # 6) Gravar no GCS
    output = dataframe_to_parquet_gcs(
        df=df,
        bucket_name=os.getenv("BUCKET_NAME"),
        prefix=f'mongo/{mongo_secret["database_name"]}/{row.COLLECTION_NAME}', #row.gcs_prefix, #ALTERAR
        file_prefix=row.COLLECTION_NAME
    )

    logger.info(f"🎯 Finalizado — Parquet salvo em: {output}")


if __name__ == "__main__":
    run()
