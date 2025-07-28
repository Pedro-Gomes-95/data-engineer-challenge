import os
import json
import logging
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv

from utils.auxiliary_functions import cast_columns, load_env_variables, flatten_schema

logger = logging.getLogger("processing_weather_codes")
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def process_weather_codes():
    """
    Processes the data regarding weather codes.

    Steps:
        1. Load the environment variables.
        2. Retrieve relevant fields for the task from the config.json.
        3. Read the loaded/weather_codes Parquet file.
        4. Strip the columns of unwanted spaces.
        5. Cast the columns to their respective types based on the configuration provided in
           the config.json file.
        6. Rename and reorder the columns.
        7. Add the ingestion date column.
        8. Save the data as Parquet to the processed/ directory.
    """

    logger.info("Starting processing of weather codes")

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

    # File to read from
    loaded_weather_codes = (
        config.get("loading_layer", {})
        .get("weather_codes", {})
        .get("table_name", "weather_codes_loaded")
    )
    loaded_weather_codes_file = loaded_files_path / f"{loaded_weather_codes}.parquet"

    # File to write to
    processed_weather_codes = (
        config.get("processing_layer", {})
        .get("weather_codes", {})
        .get("table_name", "weather_codes_processed")
    )
    processed_weather_codes_file = (
        processed_files_path / f"{processed_weather_codes}.parquet"
    )

    # Dictionary for column renaming
    columns_rename = (
        config.get("processing_layer", {})
        .get("weather_codes", {})
        .get("columns_rename", {})
    )

    # List of fields and their types for conversion
    list_fields = (
        config.get("processing_layer", {}).get("weather_codes", {}).get("fields", {})
    )

    # If the destination file exists, and the source file hasn't been updated, skip
    if os.path.exists(loaded_weather_codes_file) and os.path.exists(
        processed_weather_codes_file
    ):
        loaded_file_mdate = os.path.getmtime(loaded_weather_codes_file)
        processed_file_mdate = os.path.getmtime(processed_weather_codes_file)

        if processed_file_mdate > loaded_file_mdate:
            logger.info(
                f"Processed Parquet file is up to date. Loaded Parquet file has not been updated."
                "Skipping file processing."
            )
            return

    # Check if the file to be processed exists
    if os.path.exists(loaded_weather_codes_file):
        logger.info(f"Loading data from the Parquet file {loaded_weather_codes_file}")
        df = pd.read_parquet(loaded_weather_codes_file)
    else:
        logger.error(f"The Parquet file {loaded_weather_codes_file} was not found.")
        return

    # Clean the column names
    stripped_columns = {col: col.strip() for col in df.columns}
    df = df.rename(columns=stripped_columns, errors="ignore")

    # Cast the columns
    df = cast_columns(df=df, column_types=list_fields, logger=logger)

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
        logger.info(f"Saving the DataFrame to {processed_weather_codes_file}.")
        df.to_parquet(processed_weather_codes_file, index=False)
    except Exception as e:
        logger.error(f"Error saving the DataFrame: {e}")

    logger.info(f"Processing of weather codes finalized.")


if __name__ == "__main__":
    process_weather_codes()
