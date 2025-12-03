from pymongo import MongoClient
from urllib.parse import quote_plus
import logging

logger = logging.getLogger("mongo_to_gcs.mongo_client")

class MongoRepository:
    def __init__(self, mongo_secret: dict, collection_name: str):
        user = quote_plus(mongo_secret["database_username"])
        pwd = quote_plus(mongo_secret["database_password"])
        host = mongo_secret["database_hostname"]
        port = mongo_secret["database_port"]
        auth_db = mongo_secret["database_name"]

        uri = f"mongodb://{user}:{pwd}@{host}:{port}/?authSource={auth_db}"
        self._client = MongoClient(
            uri,
            connect=True,
            serverSelectionTimeoutMS=100_000,
            socketTimeoutMS=600_000,
            connectTimeoutMS=100_000
        )
        self._db = self._client[auth_db]
        self._collection = self._db[collection_name]

        logger.info("Conectado ao MongoDB DB=%s Collection=%s", auth_db, collection_name)

    def find(self, query, projection):
        """Extração para pipelines STANDARD"""
        return self._collection.find(query, projection)

    def aggregate(self, pipeline: list):
        """Extração para pipelines FREE"""
        return self._collection.aggregate(pipeline, allowDiskUse=True)
