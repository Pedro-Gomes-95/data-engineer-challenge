import os
import json
import logging
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv

from utils.weather_api_client import WeatherAPIClient
from utils.auxiliary_functions import load_env_variables, create_directory

logger = logging.getLogger("ingestion")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def ingest_weather_data():
    logger.info("Starting ingestion process of data from the API")

    # Load the environment variables
    path = Path(__file__).parent.parent
    env_variables = load_env_variables(path, logger)

    # Check if the API_KEY is present in the .env file, and, if not, raise an error
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError(
            "API_KEY not found in the .env file. Please insert a valid API key in the file."
        )

    # Read the configuration file, and raise an error if it's not found
    try:
        logger.info("Loading the JSON configuration file")
        with open(path / "config/config_file.json", "r") as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"Error loading the JSON configuration file: {e}")
        raise

    # Get API and City information
    base_url = (
        config.get("ingestion_layer", {})
        .get("api", {})
        .get("base_url", "https://api.openweathermap.org/data/2.5/weather")
    )
    units = config.get("ingestion_layer", {}).get("api", {}).get("units", "metric")
    language = config.get("ingestion_layer", {}).get("api", {}).get("language", "en")
    city_configuration = config.get("cities", [])

    # Get the RAW_FILES_PATH
    raw_files_path = env_variables.get("RAW_FILES_PATH")

    # API Client
    api_client = WeatherAPIClient(
        base_url=base_url,
        api_key=api_key,
        units=units,
        language=language,
        logger=logger,
    )

    # Fetch the data
    for city in city_configuration:
        logger.info(f"Fetching weather data for city {city}")
        city_weather_data = api_client.fetch_data(city=city)

        city_name = city_weather_data.get("name")
        measurement_timestamp_unix = city_weather_data.get("dt", 0)
        measurement_timestamp_string = pd.to_datetime(
            measurement_timestamp_unix, unit="s"
        ).strftime("%Y%m%d_%H%M%S")

        # Check if the directory that will store the files exists. If not, create it
        city_path = raw_files_path / city_name
        create_directory(path=city_path, logger=logger)

        # Save the file
        file_path = city_path / f"{measurement_timestamp_string}_{city_name}.json"

        with open(file_path, "w") as file:
            json.dump(city_weather_data, file, indent=4)

        logger.info(f"Data file {file_path} written successfully.")

    logger.info("Ingestion process of weather data completed successfuly.")


if __name__ == "__main__":
    ingest_weather_data()
