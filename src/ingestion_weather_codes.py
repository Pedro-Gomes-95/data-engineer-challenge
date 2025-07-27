import os
import json
import logging
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv

from utils.auxiliary_functions import load_env_variables

logger = logging.getLogger("ingestion_weather_codes")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def ingest_weather_codes():
    logging.info("Starting ingestion process of weather codes")

    # Load the environment variables
    path = Path(__file__).parent.parent
    env_variables = load_env_variables(path, logger)

    # Check if the FILES_PATH is present in the .env file
    files_path = env_variables.get("FILES_PATH")

    # Get the INTERMEDIATE_FILES_PATH
    intermediate_files_path = env_variables.get("INTERMEDIATE_FILES_PATH")

    # Read the configuration file
    logger.info("Loading the JSON configuration file")
    try:
        with open(path / "config/config_file.json", "r") as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"Error loading the JSON configuration file: {e}")
        return

    # Fetch the data
    file_name = (
        config.get("ingestion_layer", {})
        .get("weather_codes", {})
        .get("file_name", "weather_codes")
    )
    csv_path = files_path / f"{file_name}.csv"

    try:
        logger.info(f"Loading weather codes data from file {csv_path}.")
        df = pd.read_csv(csv_path, sep=",")
    except Exception as e:
        logger.error(f"Error loading the CSV file: {e}.")
        return
    
    # Add ingestion date column
    df["ingestion_date"] = pd.Timestamp.now()

    # Store the data
    weather_codes_table_name = (
        config.get("intermediate_layer", {})
        .get("weather_codes", {})
        .get("file_name", "weather_codes")
    )
    parquet_path = intermediate_files_path / f"{weather_codes_table_name}.parquet"

    try:
        logger.info(f"Saving the file to {parquet_path}")
        df.to_parquet(parquet_path, index=False)
        logging.info("Ingestion of weather codes data successful.")
    except Exception as e:
        logger.error(f"Error saving the Parquet file: {e}")

if __name__ == "__main__":
    ingest_weather_codes()
