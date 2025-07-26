import logging

from setup import setup
from ingestion import ingestion
from loading import loading

logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)

if __name__ == "__main__":
    logger.info("Starting setup process")
    setup()
    logger.info("Setup successfuly completed.")

    logger.info("Starting ingestion process")
    ingestion()
    logger.info("Ingestion successfuly completed.")

    logger.info("Starting loading process")
    loading()
    logger.info("Ingestion loading completed.")