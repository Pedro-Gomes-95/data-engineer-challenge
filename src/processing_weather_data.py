import os
import json
import logging
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv

from utils.auxiliary_functions import cast_columns, flatten_schema, load_env_variables

logger = logging.getLogger("processing_weather_data")
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def process_weather_data():
    """
    Processes the weather data.

    Steps:
        1. Load the environment variables.
        2. Retrieve relevant fields for the task from the config.json.
        3. Read the loaded/weather_data Parquet file.
        4. Cast the columns to their respective types based on the configuration provided in
           the config.json file.
        6. Rename and reorder the columns.
        7. Add the ingestion date column.
        8. Save the data as Parquet to the processed/ directory.
    """

    logger.info("Starting processing of weather data")

    # Load the environment variables
    path = Path(__file__).parent.parent
    env_variables = load_env_variables(path, logger)

    # Get the LOADED_FILES_PATH
    loaded_files_path = env_variables.get("LOADED_FILES_PATH")

    # Get the PROCESSED_FILES_PATH
    processed_files_path = env_variables.get("PROCESSED_FILES_PATH")

    # Read the configuration file
    config_path = env_variables.get("CONFIG_PATH")
    try:
        logger.info("Loading the JSON configuration file")
        with open(config_path, "r") as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"Error loading the JSON configuration file: {e}")
        return

    loaded_weather_data = (
        config.get("loading_layer", {})
        .get("weather_data", {})
        .get("table_name", "weather_data_loaded")
    )
    loaded_weather_data_file = loaded_files_path / f"{loaded_weather_data}.parquet"

    processed_weather_data = (
        config.get("processing_layer", {})
        .get("weather_data", {})
        .get("table_name", "weather_data_processed")
    )
    processed_weather_data_file = (
        processed_files_path / f"{processed_weather_data}.parquet"
    )

    columns_rename = (
        config.get("processing_layer", {})
        .get("weather_data", {})
        .get("columns_rename", {})
    )

    list_fields = (
        config.get("ingestion_layer", {}).get("weather_data", {}).get("fields", {})
    )

    if os.path.exists(loaded_weather_data_file):
        logger.info(f"Loading data from the Parquet file {loaded_weather_data_file}")
        df = pd.read_parquet(loaded_weather_data_file)
    else:
        logger.error(f"The Parquet file {loaded_weather_data_file} was not found.")
        return

    # Do schema enforcement
    # Flatten the schema
    schema_flattened = flatten_schema(schema_dict=list_fields, logger=logger)

    # Cast the columns
    df = cast_columns(df=df, column_types=schema_flattened, logger=logger)

    # Rename and reorder the columns
    if not columns_rename:
        logger.warning(
            f"No column renaming dictionary was provided in the config file."
        )
    else:
        logger.info(f"Renaming the columns")
        df = df.rename(columns=columns_rename, errors="ignore")

        try:
            logging.info("Reordering the columns")
            df = df[columns_rename.values()]
        except IndexError as e:
            logger.info(f"Error in reordering the columns: {e}")

    # Add an ingestion date column
    df["ingestion_date"] = pd.Timestamp.now()

    # Save the data
    try:
        logger.info(f"Saving the DataFrame to {processed_weather_data_file}.")
        df.to_parquet(processed_weather_data_file, index=False)
    except Exception as e:
        logger.error(f"Error saving the DataFrame: {e}")

    logger.info(f"Processing of weather data finalized.")


if __name__ == "__main__":
    process_weather_data()
