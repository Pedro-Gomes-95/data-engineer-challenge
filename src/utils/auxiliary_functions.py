import os
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv
from logging import Logger


def load_env_variables(path: Path, logger: Logger) -> dict:
    """
    Loads the environment variables in the .env file, assumed to be located
    in the path 'path'.

    Args:
        path (Path): the path containing the .env file.
        logger (Logger): logger.

    Returns:
        dict: a dictionary containing the env variables, and their values.
        If the .env file is not found, these take default
    """

    if not isinstance(path, Path):
        try:
            path = Path(path)
        except Exception as e:
            logger.error(f"Error converting {path} to Path: {e}")
            return {}

    logger.info("Loading env variables from .env file")
    env_loaded = load_dotenv(path / ".env")

    if not env_loaded:
        logger.error(
            "Could not find the .env file. Please check the README file, and create it."
            "Default values will be used for the environment variables, except the API_KEY."
        )

    return {
        "API_KEY": os.getenv("API_KEY"),
        "RAW_FILES_PATH": path / os.getenv("RAW_FILES_PATH", "data/raw"),
        "LOADED_FILES_PATH": path / os.getenv("LOADED_FILES_PATH", "data/loaded"),
        "PROCESSED_FILES_PATH": path
        / os.getenv("PROCESSED_FILES_PATH", "data/processed"),
    }


def create_directory(path: Path, logger: Logger) -> None:
    """
    Creates a directory from the provided 'path'. If it already exists,
    nothing is done.

    Args:
        path (Path): the directory to be created
        logger (Logger): logger.
    """

    if os.path.exists(path):
        logger.info(f"Directory {path} already exists.")
    else:
        logger.info(f"Creating directory {path}")
        os.makedirs(path)
        logger.info(f"Directory created.")


def flatten_json(data_json: dict, logger: Logger, parent_field: str = None) -> dict:
    """
    Flattens the dictionary 'data_json'.

    Consider the following example for the input data_json, which consists of a nested structure:
        data_json = {
            'key1': 'value1',
            'key2': {
                'key3': 'value3',
                'key4': 'value4,
                }
            ...
        }

    The returned flattened dictionary will be:
        {
            'key1': 'value1',
            'key2_key3': 'value3',
            'key2_key4': 'value4',
            ...
        }

    Args:
        data_json (dict): the input dictionary to be flattened.
        parent_field (str): for a nested structure, where one key has a dictionary
        as the value, parent_field consists of this key.
        logger (Logger): logger.

    Returns:
        flattened_json: the flattened dictionary.
    """

    logger.info("Flattening the JSON file")

    flattened_json = {}

    for key, value in data_json.items():
        full_name = f"{parent_field}_{key}" if parent_field else key

        if isinstance(value, dict):
            flattened_json.update(
                flatten_json(data_json=value, logger=logger, parent_field=full_name)
            )
        elif isinstance(value, list):
            flattened_json.update(
                flatten_json(data_json=value[0], logger=logger, parent_field=full_name)
            )
        else:
            flattened_json[full_name] = value

    return flattened_json


def flatten_schema(schema_dict: dict, logger: Logger, parent_field: str = None) -> dict:
    """
    Flattens the dictionary 'schema_dict'. This dictionary contains information about the
    data fields and their types.

    Consider the following example for the input schema_dict, which consists of a nested structure:
        schema_dict = {
            'key1': {
                'type': 'dict',
                'subfields': {
                    'key2': {
                        'type': 'float64'
                        },
                    'key3': {
                        'type': 'string'
                        }
                    }
                }
            ...
        }

    The returned flattened dictionary will be:
        {
            'key1_key2': 'float64',
            'key1_key3': 'string',
            ...
        }

    Args:
        schema_dict (dict): the input dictionary to be flattened.
        parent_field (str): for a nested structure, where one key has a dictionary
        as the value, parent_field consists of this key.
        logger (Logger): logger.

    Returns:
        flattened_json: the flattened dictionary.
    """

    logger.info(f"Flattening provided schema dictionary")

    flattened_schema = {}

    for key, value in schema_dict.items():
        column_type = value.get("type")
        full_name = f"{parent_field}_{key}" if parent_field else key

        if column_type == "dict" and "subfields" in value:
            flattened_schema.update(
                flatten_schema(
                    schema_dict=value["subfields"],
                    parent_field=full_name,
                    logger=logger,
                )
            )
        elif (
            column_type == "list" and "items" in value and "subfields" in value["items"]
        ):
            flattened_schema.update(
                flatten_schema(
                    schema_dict=value["items"]["subfields"],
                    parent_field=full_name,
                    logger=logger,
                )
            )
        elif column_type:
            flattened_schema.update({f"{full_name}": column_type})
        else:
            flattened_schema.update({f"{full_name}": None})

    return flattened_schema


def cast_columns(df: pd.DataFrame, column_types: dict, logger: Logger):
    """
    Casts the DataFrame df columns to the types specified in column_types.

    The column in the DataFrame is returned unmodified if:
        - The column does not exist in the DataFrame
        - Casting was unsuccessful

    Args:
        df (pd.DataFrame): the input DataFrame.
        column_types (str): the mapping of each to column to its type.
        logger (Logger): logger.

    Returns:
        df: the DataFrame with the columns in the desired format.
    """

    logger.info(f"Converting columns of DataFrame")

    for column, type in column_types.items():
        try:
            if type == "timestamp":
                df[column] = pd.to_datetime(df[column], unit="s")
            else:
                df[column] = df[column].astype(type, errors="ignore")
        except Exception as e:
            logger.error(f"Error converting column {column} to type {type}: {e}")
            continue

    return df


def expand_dictionary_column(
    df: pd.DataFrame, column: str, logger: Logger
) -> pd.DataFrame:
    """
    Expands the column 'column' in the DataFrame 'df' provided that 'column' is a column
    of dictionaries.

    The DataFrame is returned unmodified if:
        - The column does not exist in the DataFrame
        - The column exists, but is not of type dictionary.

    Args:
        df (pd.DataFrame): the input DataFrame.
        column (str): the column to expand into multiple columns.
        logger (Logger): logger.

    Returns:
        df: the DataFrame with the expanded column.
    """

    logger.info(f"Expanding column {column} in DataFrame")

    if column in df.columns:
        is_dict_column = df[column].apply(lambda x: isinstance(x, dict)).all()

        if is_dict_column:
            df_ = pd.json_normalize(df[column]).add_prefix(f"{column}_")
            df = pd.concat([df, df_], axis=1).drop(column, axis=1)

            logger.info(f"Column {column} succcessfuly expanded.")
        else:
            logger.error(f"The column {column} is not of type dict. Skipping.")
    else:
        logger.error("The column 'coord' could not be found in the data.")

    return df
