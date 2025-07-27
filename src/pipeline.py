import logging

from setup import setup
from ingestion_weather_data import ingest_weather_data
from ingestion_weather_codes import ingest_weather_codes
from loading import loading
from processing import processing

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

    logger.info("Ingesting weather code data")
    ingest_weather_codes()
    logger.info("Ingestion of weather codes successfuly completed.")

    logger.info("Starting ingestion process")
    ingest_weather_data()
    logger.info("Ingestion successfuly completed.")

    logger.info("Starting loading process")
    loading()
    logger.info("Ingestion loading completed.")

    logger.info("Starting processing process")
    processing()
    logger.info("Processing completed.")
