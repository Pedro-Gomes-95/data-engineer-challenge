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

    weather_data = config.get("intermediate_layer", {}).get(
        "table_name", "weather_data"
    )

    # Load the environment variables from the env file
    logger.info("Loading content from .env file")

    env_path = path / ".env"
    env_loaded = load_dotenv(env_path)

    if not env_loaded:
        raise FileNotFoundError(
            "Could not find the .env file. Please check the README file, and create the .env file."
        )
    else:
        logger.info("The .env file has been loaded successfully.")

    raw_files_path = path / os.getenv("RAW_FILES_PATH")
    if not raw_files_path:
        raw_files_path = path / "data/raw"
        logger.warning(
            f"RAW_FILES_PATH not found in the .env file, using {raw_files_path}."
        )

    intermediate_files_path = path / os.getenv("INTERMEDIATE_FILES_PATH")
    if not intermediate_files_path:
        intermediate_files_path = path / "data/intermediate"
        logger.warning(
            f"INTERMEDIATE_FILES_PATH not found in the .env file, using {intermediate_files_path}."
        )

    # Get existing data
    if os.path.exists(intermediate_files_path / f"{weather_data}.parquet"):
        logger.info(
            f"Loading data from the Parquet {intermediate_files_path}/{weather_data} file"
        )
        df = pd.read_parquet(f"{intermediate_files_path}/{weather_data}.parquet")
    else:
        logger.info(
            f"The Parquet file {intermediate_files_path}/{weather_data} was not found."
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
        os.remove(f"{intermediate_files_path}/processed_files.txt")
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
    else:
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

    for city in config["cities"]:
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
            with open(file_path, "r") as f:
                data = json.load(f)

            try:
                # Append the JSON to the dataframe
                df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)

                # Add file name as a column
                df.loc[df.index[-1], "file_name"] = file

                # Add the processed file to the list
                processed_files.append(file)

                # Increment the count of processed files
                new_files_processed += 1
                
            except Exception as e:
                logger.error(
                    f"Error processing file {file} into DataFrame: {e}. Skipping."
                )
                continue

    # Only write the Parquet file and the txt if there are new files
    if new_files_processed > 0:
        try:
            # Save the dataframe as a parquet
            logger.info(
                f"Saving the DataFrame to {intermediate_files_path}/{weather_data}.parquet"
            )
            df.to_parquet(
                intermediate_files_path / f"{weather_data}.parquet", index=False
            )
            logger.info("DataFrame saved successfully.")

            # Save the processed files list
            logger.info(
                f"Saving the processed files list to {intermediate_files_path}/processed_files.txt"
            )
            with open(intermediate_files_path / "processed_files.txt", "w") as f:
                for file in processed_files:
                    f.write(f"{file}\n")

        except Exception as e:
            logger.error(f"Error saving the DataFrame or processed files: {e}")


if __name__ == "__main__":
    loading()
