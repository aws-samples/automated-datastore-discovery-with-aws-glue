import json
import logging
import os

import boto3


LOGGER = logging.getLogger()

GLUE_TRACK_QUEUE_URL_ENV_VAR = "GLUE_TRACKING_QUEUE_URL"
DDB_EXCEPTION_NAMES_ENV_VAR = "EXCEPTION_TABLE_NAMES"

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

    _validate_field(event, "source", "aws.dynamodb")
    
    _check_missing_field(event, "detail")
    event_detail = event["detail"]
    
    _validate_field(event_detail, "eventName", "CreateTable")

    _check_missing_field(event_detail, "responseElements")
    
    return event_detail["responseElements"]


def _send_message_to_sqs(client, queue_url, message_dict):
    """Send message to SQS Queue

    :param client: Boto3 client object (SQS)
    :param queue_url: String
    :param message_dict: Dictionary

    :raises: Exception
    """
    LOGGER.info(f"Attempting to send message to: {queue_url}")
    resp = client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message_dict)
    )

    _check_missing_field(resp, "ResponseMetadata")
    resp_metadata = resp["ResponseMetadata"]

    _check_missing_field(resp_metadata, "HTTPStatusCode")
    status_code = resp_metadata["HTTPStatusCode"]
    
    if status_code == 200:
        LOGGER.info("Successfully pushed message")
    else:
        raise Exception("Unable to push message")   

   
def lambda_handler(event, context):
    """What executes when the program is run"""
    
    # configure python logger
    _configure_logger()
    # silence chatty libraries
    _silence_noisy_loggers()
    
    valid_event = _extract_valid_event(event)
    LOGGER.info("Extracted data to send to SQS")
    
    glue_queue_url = os.environ.get(GLUE_TRACK_QUEUE_URL_ENV_VAR)
    if not glue_queue_url:
        raise MissingEnvironmentVariable(
            f"{GLUE_TRACK_QUEUE_URL_ENV_VAR} environment variable is required")

    exception_tables = os.environ.get(DDB_EXCEPTION_NAMES_ENV_VAR)
    list_exception_tables = []
    if exception_tables:
        list_exception_tables = exception_tables.split(",")

    _check_missing_field(valid_event, "tableDescription")
    _check_missing_field(valid_event["tableDescription"], "tableName")
    ddb_table_name = valid_event["tableDescription"]["tableName"]
    
    if ddb_table_name in list_exception_tables:
        LOGGER.warning(f"DynamoDB table: {ddb_table_name} is in the exception list. Exiting.")
    else:
        # prepare payload for glue tracking queue
        message_dict = {}
        message_dict["data_source_type"] = "dynamodb"
        message_dict["data_source_attrs"] = valid_event
        # send message to Glue job tracking Queue
        sqs_client = boto3.client("sqs")
        _send_message_to_sqs(
            sqs_client, 
            glue_queue_url, 
            message_dict)

        sqs_client.close()
