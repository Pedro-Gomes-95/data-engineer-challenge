import os
import logging

from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger("setup")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)s, %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)


def setup():
    # os.environ.pop("API_KEY")
    os.environ.pop("RAW_FILES_PATH")
    # os.environ.pop("PROCESSED_FILES_PATH")

    path = Path(__file__).parent.parent

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

    # Check if the RAW_FILES_PATH is present in the .env file
    raw_files_path = path / os.getenv("RAW_FILES_PATH")
    if not raw_files_path:
        raw_files_path = path / "data/files"
        logger.warning(
            f"RAW_FILES_PATH not found in the .env file, using {raw_files_path} as default."
        )

    # Check if the INTERMEDIATE_FILES_PATH is present in the .env file
    intermediate_files_path = path / os.getenv("INTERMEDIATE_FILES_PATH")
    if not intermediate_files_path:
        intermediate_files_path = path / "data/intermediate"
        logger.warning(
            f"LOADED_FILES_PATH not found in the .env file, using {intermediate_files_path}."
        )

    # Check if the PROCESSED_FILES_PATH is present in the .env file
    processed_files_path = path / os.getenv("PROCESSED_FILES_PATH")
    if not processed_files_path:
        processed_files_path = path / "data/processed"
        logger.warning(
            f"PROCESSED_FILES_PATH not found in the .env file, using {processed_files_path}."
        )

    # Create the RAW_FILES_PATH if it doesn't exist
    if os.path.exists(raw_files_path):
        logger.info(f"Directory {raw_files_path} already exists.")
    else:
        logger.info(f"Creating directory {raw_files_path}")
        os.makedirs(raw_files_path)

    # Create the LOADED_FILES_PATH if it doesn't exist
    if os.path.exists(intermediate_files_path):
        logger.info(f"Directory {intermediate_files_path} already exists.")
    else:
        logger.info(f"Creating directory {intermediate_files_path}")
        os.makedirs(intermediate_files_path)

    # Create the PROCESSED_FILES_PATH if it doesn't exist
    if os.path.exists(processed_files_path):
        logger.info(f"Directory {processed_files_path} already exists.")
    else:
        logger.info(f"Creating directory {processed_files_path}")
        os.makedirs(processed_files_path)

    logger.info("Setup successful.")


if __name__ == "__main__":
    setup()
