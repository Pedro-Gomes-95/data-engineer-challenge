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

        # Validate the provided input to the class
        try:
            APIClientInputConfiguration(
                base_url=base_url,
                api_key=api_key,
                units=units,
                language=language,
            )
        except ValidationError as e:
            self.logger.error(
                f"The following error occured when validating the input of the WeatherAPIClient class: {e}"
            )
            raise

        self.base_url = base_url
        self.api_key = api_key
        self.units = units
        self.language = language

        self.logger.info("Input parameters validated successfully.")

    def build_request_url(self, city: str) -> str:
        """
        Builds the URL used to make a request to the API using the city name. 
        The request URL considered is of the form:

            https://api.openweathermap.org/data/2.5/weather? \
            q={city}&appid={api_key}&units={units}&lang={language}

        Where
            - city is the city for which weather data is to be retrieved
            - appid is the API Key
            - units is the units in which to retrieve the data (defaults to the metric system)
            - language is the language in which to retrieve the data (defaults to englis<h)

        Args:
            city (str): the city to which to fetch the weather data.

        Returns:
            str or None: the URL used to make the request to the API.
        """

        if isinstance(city, str):
            self.logger.info(f"Building URL with city name {city}")
            request_url = f"{self.base_url}?q={city}&appid={self.api_key}&units={self.units}&lang={self.language}"
        else:
            self.logger.error(f"Invalid city name provided: {city}. Returning None.")
            request_url = None

        return request_url

    def fetch_data(self, city: str) -> dict:
        """
        Fetches the data from the API for a given city.

        Args:
            city (str): the city to which to fetch the weather data.

        Returns:
            dict: the response JSON from the API, if successful. Otherwise, an empty dictionary,
        """

        self.logger.info("Fetching data from API")

        # Build the request URL
        request_url = self.build_request_url(city)

        # If it is None, return empty dictionary
        if request_url:
            response = requests.get(request_url)

            if response.status_code != 200:
                self.logger.error(
                    f"The following error occured when fetching the data: {response.status_code} - {response.text}"
                )
                result = {}
            else:
                self.logger.info("Data fetched successfully.")
                result = response.json()

        else:
            result = {}

        return result
