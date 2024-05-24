import logging
import os
import uuid

import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError


DDB_TABLE_NAME_ENV_VAR = "DDB_TABLE_NAME"
CATALOG_DB_NAME_ENV_VAR = "CATALOG_DB_NAME"

LOGGER = logging.getLogger()


class MalformedEvent(Exception):
    """Raised if a malformed event received"""
    

class MissingEnvironmentVariable(Exception):
    """Raised if a required environment variable is missing"""


def _silence_noisy_loggers():
    """Silence chatty libraries for better logging"""
    for logger in ['boto3', 'botocore',
                   'botocore.vendored.requests.packages.urllib3']:
        logging.getLogger(logger).setLevel(logging.WARNING)


def _configure_logger():
    """Configure python logger"""
    level = logging.INFO
    verbose = os.environ.get("VERBOSE", "")
    if verbose.lower() == "true":
        print("Will set the logging output to DEBUG")
        level = logging.DEBUG
    
    if len(logging.getLogger().handlers) > 0:
        # The Lambda environment pre-configures a handler logging to stderr. 
        # If a handler is already configured, `.basicConfig` does not execute. 
        # Thus we set the level directly.
        logging.getLogger().setLevel(level)
    else:
        logging.basicConfig(level=level)
        

def _check_missing_field(validation_dict, extraction_key):
    """Check if a field exists in a dictionary

    :param validation_dict: Dictionary
    :param extraction_key: String

    :raises: MalformedEvent
    """
    extracted_value = validation_dict.get(extraction_key)
    
    if not extracted_value:
        LOGGER.error(f"Missing '{extraction_key}' field in the event")
        raise MalformedEvent
    

def _validate_field(validation_dict, extraction_key, expected_value):
    """Validate the passed in field

    :param validation_dict: Dictionary
    :param extraction_key: String
    :param expected_value: String

    :raises: ValueError
    """
    extracted_value = validation_dict.get(extraction_key)

    _check_missing_field(validation_dict, extraction_key)
    
    if extracted_value != expected_value:
        LOGGER.error(f"Incorrect value found for '{extraction_key}' field")
        raise ValueError


def _extract_valid_event(event):
    """Validate incoming event and extract necessary attributes
    
    :param event: Dictionary
    
    :raises: MalformedEvent
    :raises: ValueError
    
    :rtype: Dictionary
    """
    valid_event = {}

    _validate_field(event, "source", "aws.glue")
    
    _check_missing_field(event, "detail")
    event_detail = event["detail"]
    
    _validate_field(event_detail, "eventName", "CreateTable")

    _check_missing_field(event_detail, "requestParameters")
    
    return event_detail["requestParameters"]


def lambda_handler(event, context):
    """What executes when the program is run"""
    
    # configure python logger for Lambda
    _configure_logger()
    # silence chatty libraries for better logging
    _silence_noisy_loggers()

    catalog_db_name = os.environ.get(CATALOG_DB_NAME_ENV_VAR)
    if not catalog_db_name:
        raise MissingEnvironmentVariable(
            f"{CATALOG_DB_NAME_ENV_VAR} environment variable is required")
        
    valid_event = _extract_valid_event(event)

    _check_missing_field(valid_event, "databaseName")
    if catalog_db_name != valid_event["databaseName"]:
        LOGGER.warning(f"The database is not {catalog_db_name}. Exiting.")
        return
    
    ddb_table_name = os.environ.get(DDB_TABLE_NAME_ENV_VAR)
    if not ddb_table_name:
        raise MissingEnvironmentVariable(
            f"{DDB_TABLE_NAME_ENV_VAR} environment variable is required")

    _check_missing_field(valid_event, "tableInput")
    table_input = valid_event["tableInput"]

    unique_id = uuid.uuid4()

    data_source_attrs = {}
    if table_input.get("parameters"):
        LOGGER.debug("Assigning data source attrs")
        data_source_attrs = table_input["parameters"]

    python_obj = {
        "id": str(unique_id),
        "data_source_type": "rds",
        "data_catalog_table_name": table_input["name"],
        "data_catalog_db_name": valid_event["databaseName"],
        "glue_job_created": False,
        "data_catalog_entry": True,
        "data_source_attrs": data_source_attrs,
    }
    serializer = TypeSerializer()
    ddb_client = boto3.client("dynamodb")
    try:
        resp = ddb_client.put_item(
            TableName=ddb_table_name,
            Item={
                k: serializer.serialize(v) for k, v in python_obj.items()
                },
            ConditionExpression="attribute_not_exists(id)",
        )
        LOGGER.info("Successfully initialized item in Glue Tracker DynamoDB")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            LOGGER.error(f"An entry with primary key: {unique_id} already exists")
        else:
            LOGGER.error("Unable to ")
            raise Exception

    ddb_client.close()
