import logging
import os


def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # opcional: diferenciar loggers para módulos específicos
    return logging.getLogger("mongo_to_gcs")
