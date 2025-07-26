import os
import json
import logging
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv

from utils.auxiliary_functions import cast_columns, flatten_schema

logger = logging.getLogger("processing")
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def processing():
    path = Path(__file__).parent.parent

    # Load the environment variables from the env file
    logger.info("Loading content from .env file")

    env_loaded = load_dotenv(path / ".env")

    if not env_loaded:
        raise FileNotFoundError(
            "Could not find the .env file. Please check the README file, and create the .env file."
        )
    else:
        logger.info("The .env file has been loaded successfully.")

    intermediate_files_path = path / os.getenv("INTERMEDIATE_FILES_PATH")
    if not intermediate_files_path:
        intermediate_files_path = path / "data/intermediate"
        logger.warning(
            f"INTERMEDIATE_FILES_PATH not found in the .env file, using {intermediate_files_path}."
        )

    processed_files_path = path / os.getenv("PROCESSED_FILES_PATH")
    if not processed_files_path:
        processed_files_path = path / "data/processed"
        logger.warning(
            f"PROCESSED_FILES_PATH not found in the .env file, using {processed_files_path}."
        )

    # Read the configuration file
    logger.info("Loading the JSON configuration file")

    with open(path / "config/config_file.json", "r") as f:
        config = json.load(f)

    logger.info("Configuration file loaded successfully.")

    loaded_weather_data = (
        config.get("intermediate_layer", {})
        .get("weather_data", {})
        .get("table_name", "weather_data")
    )

    processed_weather_data = (
        config.get("processing_layer", {})
        .get("weather_data", {})
        .get("table_name", "weather_data_processed")
    )

    list_fields = config.get("ingestion_layer", {}).get("api", {}).get("fields", {})

    # Get loaded data
    if os.path.exists(intermediate_files_path / f"{loaded_weather_data}.parquet"):
        logger.info(
            f"Loading data from the Parquet {intermediate_files_path}/{loaded_weather_data} file"
        )
        df = pd.read_parquet(intermediate_files_path / f"{loaded_weather_data}.parquet")
    else:
        raise FileNotFoundError(
            f"The Parquet file {intermediate_files_path}/{loaded_weather_data} was not found."
        )

    # Do schema enforcement
    # Get the flattened schema
    list_fields = config.get("ingestion_layer", {}).get(
        "api", {}).get("fields", {})
    
    schema_flattened = flatten_schema(schema_dict=list_fields, logger=logger)

    df = cast_columns(df=df, column_types=schema_flattened, logger=logger)

    # Rename and reorder the columns
    columns_rename = (
        config.get("processing_layer", {})
        .get("weather_data", {})
        .get("columns_rename", {})
    )

    if not columns_rename:
        logger.warning(
            f"No column renaming dictionary was provided in the config file."
        )
    else:
        logger.info(f"Renaming and reordering the columns")

        # Rename the columns
        df = df.rename(columns=columns_rename, errors="ignore")

        try:
            # Reorder the columns
            df = df[columns_rename.values()]
        except IndexError as e:
            logger.info(f"Error in reordering the columns: {e}")

    # Add an ingestion date column
    df["ingestion_date"] = pd.Timestamp.now()

    # Store the data
    try:
        # Save the dataframe as a parquet
        logger.info(
            f"Saving the DataFrame to {processed_files_path}/{processed_weather_data}.parquet"
        )
        df.to_parquet(
            processed_files_path / f"{processed_weather_data}.parquet", index=False
        )
        logger.info("DataFrame saved successfully.")
    except Exception as e:
        logger.error(f"Error saving the DataFrame: {e}")


if __name__ == "__main__":
    processing()
