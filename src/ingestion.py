import os
import json
import logging
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv
from utils.weather_api_client import WeatherAPIClient

logger = logging.getLogger("ingestion")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)

def main():
    os.environ.pop("API_KEY")
    os.environ.pop("RAW_FILES_PATH")

    path = Path(__file__).parent.parent

    # Read the configuration file
    logger.info("Loading the JSON configuration file")

    with open(path / "config/config_file.json", "r") as f:
        config = json.load(f)

    # Get API and City information
    api_configuration = config.get("api", {})
    city_configuration = config.get("cities", [])

    # Load the environment variables from the .env file
    logger.info("Loading content from .env file")

    env_path = path / ".env"
    env_loaded = load_dotenv(env_path)

    if not env_loaded:
        raise FileNotFoundError(
            "Could not find the .env file. Please check the README file, and create the .env file."
        )
    else:
        logger.info("The .env file has been loaded successfully.")

    # Check if the API_KEY is present in the .env file
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError(
            "API_KEY not found in the .env file. Please insert a valid API key in the file."
        )

    # Check if the RAW_FILES_PATH and LOADED_FILES_PATH are present in the .env file
    raw_files_path = path / os.getenv("RAW_FILES_PATH")
    if not raw_files_path:
        raw_files_path = path / "data/files"
        logger.warning(
            f"RAW_FILES_PATH not found in the .env file, using {raw_files_path} as default."
        )

    logger.info(".env file loaded successfully.")

    # Create the API Client
    api_client = WeatherAPIClient(
        base_url=api_configuration.get(
            "base_url", "https://api.openweathermap.org/data/2.5/weather"
        ),
        api_key=api_key,
        units=api_configuration.get("units", "metric"),
        language=api_configuration.get("language", "en"),
        logger=logger,
    )

    # Fetch the data
    for city in city_configuration:
        city_weather_data = api_client.fetch_data(city=city)

        city_name = city_weather_data.get("name")
        measurement_timestamp_unix = city_weather_data.get("dt", 0)
        measurement_timestamp_string = pd.to_datetime(
            measurement_timestamp_unix, unit="s"
        ).strftime("%Y%m%d_%H%M%S")

        # Check if the directory that will store the files exists
        city_path = raw_files_path / city_name

        try:
            os.makedirs(city_path)
            logging.info(f"Creating directory {city_path}")
        except FileExistsError:
            logging.info(f"Directory {city_path} already exists.")

        file_path = city_path / f"{measurement_timestamp_string}_{city_name}.json"

        with open(file_path, "w") as file:
            json.dump(city_weather_data, file, indent=4)

        logging.info(f"Data file {file_path} created successfully")


if __name__ == "__main__":
    main()
