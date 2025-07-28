import os
import sys
import json
import logging
import pandas as pd

from pathlib import Path

# Add parent directory to sys.path to get the functions in utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.auxiliary_functions import load_env_variables

logger = logging.getLogger("loading_weather_codes")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def load_weather_codes():
    """
    Loads the data regarding weather codes into a Parquet file. The data is read from a CSV file
    generated from the information available in https://openweathermap.org/weather-conditions

    Steps:
        1. Load the environment variables.
        2. Retrieve relevant fields for the task from the config.json.
        3. Check if the source and destination file exist. If both exist, and the source file
           has not been updated, skip processing. Otherwise, continue with the processing.
        3. Read the CSV file containing weather code information as a DataFrame.
        4. Add an ingestion_date column, indicating the moment the data was processed.
        5. Save the DataFrame to a Parquet file under the data/loaded folder.
    """

    logging.info("Starting ingestion process of weather codes")

    # Load the environment variables
    path = Path(__file__).parent.parent.parent
    env_variables = load_env_variables(path, logger)

    # Check if the RAW_WEATHER_CODES_PATH is present in the .env file
    raw_files_path = env_variables.get("RAW_WEATHER_CODES_PATH")

    # Get the LOADED_FILES_PATH
    loaded_files_path = env_variables.get("LOADED_FILES_PATH")

    # Read the configuration file
    config_path = env_variables.get("CONFIG_PATH")
    try:
        logger.info("Loading the JSON configuration file")
        with open(config_path, "r") as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"Error loading the JSON configuration file: {e}")
        return

    # Get the directories of the source and destination files
    weather_codes_file_name = (
        config.get("ingestion_layer", {})
        .get("weather_codes", {})
        .get("file_name", "weather_codes.csv")
    )
    csv_path = raw_files_path / weather_codes_file_name

    weather_codes_table_name = (
        config.get("loading_layer", {})
        .get("weather_codes", {})
        .get("table_name", "weather_codes_loaded")
    )
    parquet_path = loaded_files_path / f"{weather_codes_table_name}.parquet"

    # If the destination file exists, and the source file hasn't been updated, skip
    if os.path.exists(csv_path) and os.path.exists(parquet_path):
        json_path_mdate = os.path.getmtime(csv_path)
        parquet_file_mdate = os.path.getmtime(parquet_path)

        if parquet_file_mdate > json_path_mdate:
            logger.info(
                f"Parquet file is up to date. Original file has not been updated."
                "Skipping file processing."
            )
            return

    # Fetch the data
    try:
        logger.info(f"Loading weather codes data from file {csv_path}.")
        df = pd.read_csv(csv_path, sep=",")
    except Exception as e:
        logger.error(f"Error loading the CSV file: {e}.")
        return

    # Add ingestion date column
    df["ingestion_date"] = pd.Timestamp.now()

    # Store the data
    try:
        logger.info(f"Saving the file to {parquet_path}")
        df.to_parquet(parquet_path, index=False)
    except Exception as e:
        logger.error(f"Error saving the Parquet file: {e}")

    logger.info(f"Ingestion of weather codes finalized.")


if __name__ == "__main__":
    load_weather_codes()
