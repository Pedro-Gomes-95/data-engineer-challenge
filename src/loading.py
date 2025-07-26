import os
import json
import logging
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger("loading")
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def loading():
    # os.environ.pop("API_KEY")
    os.environ.pop("RAW_FILES_PATH")
    # os.environ.pop("PROCESSED_FILES_PATH")

    path = Path(__file__).parent.parent

    # Read the configuration file
    logger.info("Loading the JSON configuration file")

    with open(path / "config/config_file.json", "r") as f:
        config = json.load(f)

    logger.info("Configuration file loaded successfully.")

    weather_table_name = config.get("intermediate_layer", {}).get(
        "table_name", "weather_data"
    )

    # Load the environment variables from the env file
    logger.info("Loading content from .env file")

    env_loaded = load_dotenv(path / ".env")

    if not env_loaded:
        raise FileNotFoundError(
            "Could not find the .env file. Please check the README file, and create the .env file."
        )
    else:
        logger.info("The .env file has been loaded successfully.")

    # Check if the RAW_FILES_PATH is present in the .env file
    raw_files_path_env = os.getenv("RAW_FILES_PATH")

    if raw_files_path_env:
        raw_files_path = path / os.getenv("RAW_FILES_PATH")
    else:
        raw_files_path = path / "data/raw"
        logger.warning(
            f"RAW_FILES_PATH not found in the .env file, using {raw_files_path} as default."
        )

    # Check if the INTERMEDIATE_FILES_PATH is present in the .env file
    intermediate_files_path_env = os.getenv("INTERMEDIATE_FILES_PATH")

    if intermediate_files_path_env:
        intermediate_files_path = path / os.getenv("INTERMEDIATE_FILES_PATH")
    else:
        intermediate_files_path = path / "data/intermediate"
        logger.warning(
            f"LOADED_FILES_PATH not found in the .env file, using {intermediate_files_path}."
        )

    # Get existing data
    if os.path.exists(intermediate_files_path / f"{weather_table_name}.parquet"):
        logger.info(
            f"Loading data from the Parquet {intermediate_files_path}/{weather_table_name} file"
        )
        df = pd.read_parquet(intermediate_files_path / f"{weather_table_name}.parquet")
    else:
        logger.info(
            f"The Parquet file {intermediate_files_path}/{weather_table_name} was not found."
        )
        df = pd.DataFrame()

    # Get the list of processed files
    if os.path.exists(intermediate_files_path / "processed_files.txt"):
        logger.info("Loading processed files from the processed_files.txt file")
        with open(intermediate_files_path / "processed_files.txt", "r") as f:
            processed_files = f.read().splitlines()
    else:
        logger.info(
            f"The file {intermediate_files_path}/processed_files.txt was not found."
        )
        processed_files = []

    # If data does not exist but the text file does, delete the text file and restart everything
    if df.empty and processed_files:
        logger.info(
            "Parquet file does not exist but processed_files.txt exists. Deleting processed_files.txt and starting from scratch."
        )
        os.remove(intermediate_files_path / "processed_files.txt")
        processed_files = []

    # If data does exist but the text file does not, use the file_name column to know processed_files
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
    elif (not df.empty) and processed_files:
        if "file_name" in df.columns:
            processed_files_in_parquet = set(df["file_name"].unique().tolist())
            processed_files_in_txt = set(processed_files)

            if len(processed_files_in_parquet) > len(processed_files_in_txt):
                logger.info(
                    "There are more processed files in the Parquet file than in the text file. Updating the text file."
                )

                difference_processed_files = (
                    processed_files_in_parquet - processed_files_in_txt
                )
                with open(intermediate_files_path / "processed_files.txt", "a") as f:
                    for file in difference_processed_files:
                        f.write(f"{file}\n")

                processed_files.extend(difference_processed_files)

            elif len(processed_files_in_parquet) < len(processed_files_in_txt):
                logger.info(
                    "There are more processed files in the text file than in the Parquet file. These will be deleted from the text file."
                )

                with open(intermediate_files_path / "processed_files.txt", "w") as f:
                    for file in processed_files_in_parquet:
                        f.write(f"{file}\n")

                processed_files = processed_files_in_parquet
            else:
                logger.info(
                    "The processed files in the Parquet file and the text file are matching."
                )

    # Iterate through the different city directories and process the files to a single table
    new_files_processed = 0
    new_files_list = []
    new_file_names = []
    new_files_ingestion_timestamps = []

    for city in config["cities"]:
        logger.info(f"Processing files for city {city}")

        city_name = city["name"]
        files_path = raw_files_path / city_name

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
            file_path = files_path / file

            logger.info(f"Processing file {file_path}")

            try:
                with open(file_path, "r") as f:
                    data = json.load(f)

                new_files_list.append(data)
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
        new_files_df = pd.DataFrame(new_files_list)
        new_files_df["file_name"] = new_file_names
        new_files_df["ingestion_date"] = new_files_ingestion_timestamps

        # Append to the existing data
        df = pd.concat([df, new_files_df], ignore_index=True)

        try:
            # Save the dataframe as a parquet
            logger.info(
                f"Saving the DataFrame to {intermediate_files_path}/{weather_table_name}.parquet"
            )
            df.to_parquet(
                intermediate_files_path / f"{weather_table_name}.parquet", index=False
            )
            logger.info("DataFrame saved successfully.")

            # Save the processed files list
            logger.info(
                f"Saving the processed files list to {intermediate_files_path}/processed_files.txt"
            )
            with open(intermediate_files_path / "processed_files.txt", "a") as f:
                for file in new_file_names:
                    f.write(f"{file}\n")

            logger.info(f"Successfuly loaded {new_files_processed} new files.")

        except Exception as e:
            logger.error(f"Error saving the DataFrame or processed files: {e}")


if __name__ == "__main__":
    loading()
