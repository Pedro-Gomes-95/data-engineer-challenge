import requests
import logging

from pydantic import ValidationError
from utils.input_configuration import APIClientInputConfiguration


class WeatherAPIClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        units: str = "metric",
        language: str = "en",
        logger: logging.Logger = None,
    ):
        self.logger = (
            logger
            if isinstance(logger, logging.Logger)
            else logging.getLogger(__name__)
        )
        self.logger.info("Validating input parameters")

        try:
            APIClientInputConfiguration(
                base_url=base_url,
                api_key=api_key,
                units=units,
                language=language,
            )
        except ValidationError as e:
            self.logger.error(f"Validation error: {e}")
            raise

        self.base_url = base_url
        self.api_key = api_key
        self.units = units
        self.language = language

        self.logger.info("Input parameters validated successfully.")

    def build_request_url(self, city: dict) -> str:
        if not isinstance(city, dict):
            raise TypeError(
                f"Expected 'city' to be a dictionary, but got: {type(city).__name__} instead"
            )

        city_name = city.get("name")

        if city_name:
            self.logger.info("City name provided, building URL with city name.")
            request_url = f"{self.base_url}?q={city_name}&appid={self.api_key}&units={self.units}&lang={self.language}"
        else:
            raise ValueError("No valid city information provided.")

        return request_url

    def fetch_data(self, city: dict) -> dict:
        self.logger.info("Fetching data from API")
        request_url = self.build_request_url(city)
        response = requests.get(request_url)

        if response.status_code != 200:
            raise Exception(
                f"Error fetching data: {response.status_code} - {response.text}"
            )

        self.logger.info("Data fetched successfully.")
        return response.json()
