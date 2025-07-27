import logging

from pathlib import Path
from utils.auxiliary_functions import load_env_variables, create_directory

logger = logging.getLogger("setup")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def setup():
    """
    Sets up the environment variables and creates the directories that will
    store the data.

    Steps:
        1. Load the environment variables.
        2. Create the RAW_FILES_PATH
            Contains the JSON files resulting from the API calls to the Weather API.
        3. Create the LOADING_FILES_PATH
            Contains the data loaded from the raw files in the form of Parquet files.
        4. Create the PROCESSED_FILES_PATH
            Contains the clean Parquet files.

    Raises:
        ValueError: if no API_KEY is provided in the .env file, an error is raised.
    """

    logger.info("Starting setup process")

    # Load the environment variables
    path = Path(__file__).parent.parent
    env_variables = load_env_variables(path, logger)

    # Check if the API_KEY is present
    api_key = env_variables.get("API_KEY")
    if not api_key:
        raise ValueError(
            "API_KEY not found in the .env file. Please insert a valid API key in the file."
        )

    # Create the paths if they don't exist
    for value in env_variables.values():
        if isinstance(value, Path):
            create_directory(path=value, logger=logger)

    logger.info("Setup successfuly completed.")


if __name__ == "__main__":
    setup()
