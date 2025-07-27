import json
import logging
import pandas as pd

from pathlib import Path
from utils.auxiliary_functions import load_env_variables, expand_dictionary_column

logger = logging.getLogger("loading_city_codes")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def load_city_codes():
    """
    Loads the data regarding cities into a Parquet file. The data is read from a JSON file
    provided by OpenWeather, you can find it here: https://bulk.openweathermap.org/sample/

    Steps:
        1. Load the environment variables.
        2. Retrieve relevant fields for the task from the config.json.
        3. Read the JSON file containing city information as a DataFrame.
        4. Flatten the 'coord' column into separate columns.
        5. Add an ingestion_date column, indicating the moment the data was processed.
        6. Save the DataFrame to a Parquet file.
    """

    logging.info("Starting loading process of city codes")

    # Load the environment variables
    path = Path(__file__).parent.parent
    env_variables = load_env_variables(path, logger)

    # Check if the RAW_CITY_CODES_PATH is present in the .env file
    raw_files_path = env_variables.get("RAW_CITY_CODES_PATH")

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
    
    # Fetch the data
    file_name = (
        config.get("ingestion_layer", {})
        .get("city_codes", {})
        .get("file_name", "city_codes.json")
    )
    json_path = raw_files_path / file_name

    try:
        logger.info(f"Loading city codes data from file {json_path}.")
        with open(json_path) as f:
            df = pd.read_json(json_path)
    except Exception as e:
        logger.error(f"Error loading the JSON file: {e}.")
        return

    # Flatten the DataFrame's dictionary columns
    df = expand_dictionary_column(df=df, column="coord", logger=logger)

    # Add ingestion date column
    df["ingestion_date"] = pd.Timestamp.now()

    # Store the data
    city_codes_table_name = (
        config.get("loading_layer", {})
        .get("city_codes", {})
        .get("table_name", "city_codes")
    )
    parquet_path = loaded_files_path / f"{city_codes_table_name}.parquet"

    try:
        logger.info(f"Saving the file to {parquet_path}")
        df.to_parquet(parquet_path, index=False)
    except Exception as e:
        logger.error(f"Error saving the Parquet file: {e}")

    logger.info(f"Loading of city codes finalized.")

if __name__ == "__main__":
    load_city_codes()
