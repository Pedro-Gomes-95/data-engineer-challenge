import logging
import pandas as pd


def flatten_json(
    data_json: dict, logger: logging.Logger, parent_field: str = None
) -> dict:
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


def flatten_schema(
    schema_dict: dict, logger: logging.Logger, parent_field: str = None
) -> dict:
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


def cast_columns(df: pd.DataFrame, column_types: str, logger: logging.Logger):
    logger.info(f"Renaming columns of DataFrame")

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
