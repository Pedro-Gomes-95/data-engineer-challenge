import os
import json
import logging
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv
from utils.auxiliary_functions import expand_dictionary_column

logger = logging.getLogger("ingestion_city_codes")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def ingest_city_codes():
    path = Path(__file__).parent.parent

    # Read the configuration file
    logger.info("Loading the JSON configuration file")
    try:
        with open(path / "config/config_file.json", "r") as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"Error loading the JSON configuration file: {e}")
        return

    # Load the environment variables from the .env file
    logger.info("Loading content from .env file")
    env_loaded = load_dotenv(path / ".env")

    if not env_loaded:
        logger.error(
            "Could not find the .env file. Please check the README file, and create the .env file."
        )
        return
    else:
        logger.info("The .env file has been loaded successfully.")

    # Check if the FILES_PATH is present in the .env file
    files_path_env = os.getenv("FILES_PATH")

    if files_path_env:
        files_path = path / files_path_env
    else:
        files_path = path / "files"
        logger.warning(
            f"FILES_PATH not found in the .env file, using {files_path} as default."
        )

    # Check if the INTERMEDIATE_FILES_PATH is present in the .env file
    intermediate_files_path_env = os.getenv("INTERMEDIATE_FILES_PATH")

    if intermediate_files_path_env:
        intermediate_files_path = path / intermediate_files_path_env
    else:
        intermediate_files_path = path / "data/intermediate"
        logger.warning(
            f"LOADED_FILES_PATH not found in the .env file, using {intermediate_files_path}."
        )

    logger.info(".env file loaded successfully.")

    # Fetch the data
    file_name = (
        config.get("ingestion_layer", {})
        .get("city_codes", {})
        .get("file_name", "city_codes")
    )
    json_path = files_path / f"{file_name}.json"

    try:
        logger.info(f"Loading weather codes data from file {json_path}.")
        with open(json_path) as f:
            df = pd.read_json(json_path)
    except Exception as e:
        logger.error(f"The file {json_path} was not found.")
        return

    # Flatten the DataFrame's dictionary
    df = expand_dictionary_column(df=df, column="coord", logger=logger)

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
        logger.info("Parquet file saved successfully.")
    except Exception as e:
        logger.error(f"Error saving the file: {e}")


if __name__ == "__main__":
    ingest_city_codes()
