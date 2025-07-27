import json
import logging
import pandas as pd

from pathlib import Path
from utils.auxiliary_functions import load_env_variables, expand_dictionary_column

logger = logging.getLogger("ingestion_city_codes")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def ingest_city_codes():
    logging.info("Starting ingestion process of city codes")

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
        .get("city_codes", {})
        .get("file_name", "city_codes")
    )
    json_path = files_path / f"{file_name}.json"

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
        config.get("intermediate_layer", {})
        .get("city_codes", {})
        .get("file_name", "city_codes")
    )
    parquet_path = intermediate_files_path / f"{city_codes_table_name}.parquet"

    try:
        logger.info(f"Saving the file to {parquet_path}")
        df.to_parquet(parquet_path, index=False)
        logging.info("Ingestion of city codes data successful.")
    except Exception as e:
        logger.error(f"Error saving the Parquet file: {e}")

    logger.info(f"Ingestion of city codes finalized.")

if __name__ == "__main__":
    ingest_city_codes()
