import logging

from setup.setup import setup

from ingestion.ingestion_weather_data import ingest_weather_data

from loading.loading_weather_codes import load_weather_codes
from loading.loading_city_codes import load_city_codes
from loading.loading_weather_data import load_weather_data

from processing.processing_weather_data import process_weather_data
from processing.processing_weather_codes import process_weather_codes
from processing.processing_city_codes import process_city_codes

logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)

if __name__ == "__main__":
    logger.info("Starting the pipeline")
    setup()
    ingest_weather_data()
    load_weather_data()
    load_weather_codes()
    load_city_codes()
    process_weather_data()
    process_weather_codes()
    process_city_codes()
    logger.info("Pipeline completed.")
