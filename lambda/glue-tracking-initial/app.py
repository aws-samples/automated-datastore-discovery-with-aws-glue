import json
import logging
import os
import uuid

import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError


DDB_TABLE_NAME_ENV_VAR = "GLUE_TRACKER_TABLE_NAME"
SQS_QUEUE_URL_ENV_VAR = "SQS_QUEUE_URL"

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


def _get_sqs_message_attributes(event):
    """Extract receiptHandle from message
    
    :param event: Dictionary
    
    :raises: MalformedEvent
    
    :rtype: Dictionary
    """
    LOGGER.info("Attempting to extract receiptHandle from SQS")
    records = event.get("Records")
    if not records:
        LOGGER.warning("No receiptHandle found, probably not an SQS message")
        return
    try:
        first_record = records[0]
    except IndexError:
        raise MalformedEvent("Records seem to be empty")
    
    receipt_handle = first_record.get("receiptHandle")
    message_id = first_record.get("messageId")
    
    if not receipt_handle:
        raise MalformedEvent("No receiptHandle present")
    
    if not message_id:
        raise MalformedEvent("No messageId present")
    
    return {
        "message_id": message_id,
        "receipt_handle": receipt_handle
    }


def _get_message_body(event):
    """Extract message body from the event
    
    :param event: Dictionary
    
    :raises: MalformedEvent
    
    :rtype: Dictionary
    """
    body = ""
    test_event = event.get("test_event", "")
    if test_event.lower() == "true":
        LOGGER.info("processing test event (and not from SQS)")
        LOGGER.debug("Test body: %s", event)
        return event
    else:
        LOGGER.info("Attempting to extract message body from SQS")
        records = event.get("Records")
        if not records:
            raise MalformedEvent("Missing 'Records' field in the event")
        
        first_record = records[0]
        
        try:
            body = first_record.get("body")
        except AttributeError:
            raise MalformedEvent("First record is not a proper dict")
        
        if not body:
            raise MalformedEvent("Missing 'body' in the record")
            
        try:
            return json.loads(body)
        except json.decoder.JSONDecodeError:
            raise MalformedEvent("'body' is not valid JSON")
            

def _get_sqs_message_attributes(event):
    """Extract receiptHandle from message
    
    :param event: Dictionary
    
    :raises: MalformedEvent
    
    :rtype: Dictionary
    """
    LOGGER.info("Attempting to extract receiptHandle from SQS")
    records = event.get("Records")
    if not records:
        LOGGER.warning("No receiptHandle found, probably not an SQS message")
        return
    try:
        first_record = records[0]
    except IndexError:
        raise MalformedEvent("Records seem to be empty")
    
    receipt_handle = first_record.get("receiptHandle")
    message_id = first_record.get("messageId")
    
    if not receipt_handle:
        raise MalformedEvent("No receiptHandle present")
    
    if not message_id:
        raise MalformedEvent("No messageId present")
    
    return {
        "message_id": message_id,
        "receipt_handle": receipt_handle
    }


def _delete_sqs_message(msg_attr):
    """Delete message from queue (lest it remain in the queue forever)

    :param msg_attr: Dictionary

    :raises: MissingEnvironmentVariable
    :raises: Exception
    """
    LOGGER.info(f"Deleting message {msg_attr['message_id']} from sqs")
    sqs_client = boto3.client("sqs")
    queue_url = os.environ.get(SQS_QUEUE_URL_ENV_VAR)
    if not queue_url:
        raise MissingEnvironmentVariable(
            f"{SQS_QUEUE_URL_ENV_VAR} environment variable is required")
            
    deletion_resp = sqs_client.delete_message(
        QueueUrl=queue_url, ReceiptHandle=msg_attr["receipt_handle"])
    
    sqs_client.close()

    resp_metadata = deletion_resp.get("ResponseMetadata")
    if not resp_metadata:
        raise Exception("No response metadata from deletion call")
    status_code = resp_metadata.get("HTTPStatusCode")
    
    if status_code == 200:
        LOGGER.info(f"Successfully deleted message")
    else:
        raise Exception("Unable to delete message")
        

def lambda_handler(event, context):
    """What executes when the program is run"""
    
    # configure python logger for Lambda
    _configure_logger()
    # silence chatty libraries for better logging
    _silence_noisy_loggers()
    
    msg_attr = _get_sqs_message_attributes(event)
    
    if msg_attr:
        # Because messages remain in the queue
        _delete_sqs_message(msg_attr)

    # TODO: figure out DLQ mechanism or create another queue for unprocessed stuff
    # or delete the message after the item is written successfully in DDB

    body = _get_message_body(event)

    # put record in Glue Job Tracker DDB Table
    ddb_table_name = os.environ.get(DDB_TABLE_NAME_ENV_VAR)
    if not ddb_table_name:
        raise MissingEnvironmentVariable(
            f"{DDB_TABLE_NAME_ENV_VAR} environment variable is required")

    ddb_client = boto3.client("dynamodb")
    unique_id = uuid.uuid4()

    python_obj = {
        "id": str(unique_id),
        "data_source_type": body["data_source_type"],
        "glue_job_created": False,
        "data_catalog_entry": False,
        "data_source_attrs": body["data_source_attrs"]
    }
    serializer = TypeSerializer()
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
