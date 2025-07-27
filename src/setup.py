import os
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
    # Load the environment variables
    path = Path(__file__).parent.parent
    env_variables = load_env_variables(path, logger)

    # Check if the API_KEY is present
    api_key = env_variables.get("API_KEY")
    if not api_key:
        raise ValueError(
            "API_KEY not found in the .env file. Please insert a valid API key in the file."
        )

    # Check if the RAW_FILES_PATH is present in the .env file
    raw_files_path = env_variables.get("RAW_FILES_PATH")

    # Check if the INTERMEDIATE_FILES_PATH is present in the .env file
    intermediate_files_path = env_variables.get("INTERMEDIATE_FILES_PATH")

    # Check if the PROCESSED_FILES_PATH is present in the .env file
    processed_files_path = env_variables.get("PROCESSED_FILES_PATH")

    # Create the RAW_FILES_PATH if it doesn't exist
    create_directory(path=raw_files_path, logger=logger)

    # Create the LOADED_FILES_PATH if it doesn't exist
    create_directory(path=intermediate_files_path, logger=logger)

    # Create the PROCESSED_FILES_PATH if it doesn't exist
    create_directory(path=processed_files_path, logger=logger)

    logger.info("Setup successful.")


if __name__ == "__main__":
    setup()
