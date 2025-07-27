import os
import json
import logging
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv

from utils.auxiliary_functions import flatten_json, flatten_schema, load_env_variables

logger = logging.getLogger("loading")
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def load_weather_data():
    logger.info("Starting loading process of weather data from the API")

    # Load the environment variables
    path = Path(__file__).parent.parent
    env_variables = load_env_variables(path, logger)

    # Get the RAW_FILES_PATH
    raw_files_path = env_variables.get("RAW_FILES_PATH")

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

    weather_table_name = (
        config.get("intermediate_layer", {})
        .get("weather_data", {})
        .get("table_name", "weather_data")
    )

    # Get existing data
    file_path = intermediate_files_path / f"{weather_table_name}.parquet"

    if os.path.exists(file_path):
        logger.info(f"Loading data from the Parquet {file_path} file")
        df = pd.read_parquet(file_path)
    else:
        logger.info(f"The Parquet file {file_path} was not found.")
        df = pd.DataFrame()

    # Get the list of processed files
    text_file_path = intermediate_files_path / "processed_files.txt"

    if os.path.exists(text_file_path):
        logger.info(f"Loading processed files from the processed_files.txt file.")
        with open(text_file_path, "r") as f:
            processed_files = f.read().splitlines()
    else:
        logger.info(f"The file {text_file_path} was not found.")
        processed_files = []

    # If the data does not exist but the text file does, delete the text file and load all data
    if df.empty and processed_files:
        logger.info(
            "Parquet file does not exist but processed_files.txt exists. Deleting processed_files.txt and starting from scratch."
        )
        os.remove(text_file_path)
        processed_files = []

    # If the data does exist but the text file does not, use the file_name column in the Parquet file to infer the the processed files
    # If the column is not present in the data, load all data
    elif (not df.empty) and (not processed_files):
        if "file_name" not in df.columns:
            logger.info(
                "Parquet file exists but processed_files.txt is empty and 'file_name' column is missing from the data. Cannot determine processed files. Starting from scratch."
            )
            df = pd.DataFrame()
            processed_files = []
        else:
            logger.info(
                "Parquet file exists but processed_files.txt is empty. Getting processed files from the Parquet file."
            )
            processed_files = df["file_name"].unique().tolist()

    # If both exist, check if there is a mismatch in the processed files between both
    elif (not df.empty) and processed_files:
        if "file_name" in df.columns:
            processed_files_in_parquet = set(df["file_name"].unique().tolist())
            processed_files_in_txt = set(processed_files)

            difference_parquet_txt = processed_files_in_parquet - processed_files_in_txt
            difference_txt_parquet = processed_files_in_txt - processed_files_in_parquet

            # Corner case 1: there are more processed files in the Parquet than those listed in the txt file
            if difference_parquet_txt:
                logger.info(
                    "There are more processed files in the Parquet file than in the text file. Updating the text file."
                )

                with open(text_file_path, "a") as f:
                    for file in difference_parquet_txt:
                        f.write(f"{file}\n")

                processed_files.extend(difference_parquet_txt)

            # Corner case 2: there are more processed files in the txt file than in the Parquet
            elif difference_txt_parquet:
                logger.info(
                    "There are more processed files in the text file than in the Parquet file. These will be deleted from the text file."
                )

                with open(text_file_path, "w") as f:
                    for file in processed_files_in_parquet:
                        f.write(f"{file}\n")

                processed_files = processed_files_in_parquet

            else:
                logger.info(
                    "The processed files in the Parquet file and the text file match."
                )

    # Get the flattened schema
    list_fields = config.get("ingestion_layer", {}).get("api", {}).get("fields", {})
    schema_flattened = flatten_schema(schema_dict=list_fields, logger=logger)

    # Iterate through the different city directories and process the files to a single table
    new_files_processed = 0
    new_files_list = []
    new_file_names = []
    new_files_ingestion_timestamps = []

    for city in config["cities"]:
        logger.info(f"Processing files for city: {city}")
        files_path = raw_files_path / city

        # If the directory does not exist, skip loading
        if not os.path.exists(files_path):
            logger.error(f"The directory {files_path} does not exist. Skipping.")
            continue
        else:
            logger.info(f"Processing files in the directory {files_path}.")

        # Identify the new files
        files = set([f for f in os.listdir(files_path) if f.endswith(".json")])
        new_files = set(files - set(processed_files))

        # Iterate through new files
        for file in new_files:
            logger.info(f"Processing file {file_path}")
            file_path = files_path / file

            try:
                with open(file_path, "r") as f:
                    data = json.load(f)

                # Flatten the JSON structure
                data_flattened = flatten_json(data_json=data, logger=logger)

                # Check if any fields are missing from the API response
                missing_fields = set(schema_flattened) - set(data_flattened)

                # If the fields are missing, create them with None
                for field in missing_fields:
                    data_flattened[field] = None

                # Append
                new_files_list.append(data_flattened)
                new_file_names.append(file)
                new_files_ingestion_timestamps.append(pd.Timestamp.now())
                new_files_processed += 1

            except Exception as e:
                logger.error(
                    f"Error processing file {file} into DataFrame: {e}. Skipping."
                )
                continue

    # If new files exist, write the data and the txt file
    if new_files_list:
        # To avoid conversion problems, convert everything to string
        new_files_df = pd.DataFrame(new_files_list).astype("string")

        new_files_df["file_name"] = new_file_names
        new_files_df["ingestion_date"] = new_files_ingestion_timestamps

        # Append to the existing data
        df = pd.concat([df, new_files_df], ignore_index=True)

        try:
            # Save the dataframe as a parquet
            logger.info(f"Saving the DataFrame to {file_path}.")
            df.to_parquet(file_path, index=False)
            logger.info("DataFrame saved successfully.")

            # Save the processed files list
            logger.info(f"Saving the processed files list to {text_file_path}.")
            with open(text_file_path, "a") as f:
                for file in new_file_names:
                    f.write(f"{file}\n")
            logger.info(f"Successfuly loaded {new_files_processed} new files.")

        except Exception as e:
            logger.error(f"Error saving the DataFrame or processed files: {e}")

    logger.info("Loading process of weather data from the API finalized.")

if __name__ == "__main__":
    load_weather_data()
