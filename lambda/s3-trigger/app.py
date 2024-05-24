import json
import logging
import os

import boto3
from botocore.exceptions import ClientError


LOGGER = logging.getLogger()

GLUE_TRACK_QUEUE_URL_ENV_VAR = "GLUE_TRACKING_QUEUE_URL"
GLUE_CUSTOM_ENTITY_QUEUE_URL_ENV_VAR = "GLUE_CUSTOM_ENTITY_QUEUE_URL"

VALID_BUCKET_TAG_KEY = "gdpr-scan"
VALID_BUCKET_TAG_VALUE = "true"

VALID_CUSTOM_ENTITY_TAG_KEY = "glue-custom-entity"
VALID_CUSTOM_ENTITY_TAG_VALUE = "true"


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

    _validate_field(event, "source", "aws.s3")
    
    _check_missing_field(event, "detail")
    event_detail = event["detail"]
    
    _validate_field(event_detail, "eventName", "CreateBucket")

    _check_missing_field(event_detail, "requestParameters")

    request_params = event_detail["requestParameters"]

    _check_missing_field(request_params, "bucketName")
    
    return request_params


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


def _get_bucket_tags(client, bucket_name):
    """Get tags for the passed in bucket_name

    :param client: Boto3 client object (SQS)
    :param queue_url: String
    :param message_dict: Dictionary

    :raises: Exception
    """
    LOGGER.info(f"Attempting fetch tags for S3 bucket: {bucket_name}")
    try:
        resp = client.get_bucket_tagging(Bucket=bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchTagSet":
            LOGGER.warning("Bucket does not have any tags")
            return
        else:
            LOGGER.error("Unable to fetch tags for this bucket")
            raise Exception

    _check_missing_field(resp, "ResponseMetadata")
    resp_metadata = resp["ResponseMetadata"]

    _check_missing_field(resp_metadata, "HTTPStatusCode")
    status_code = resp_metadata["HTTPStatusCode"]
    
    if status_code == 200:
        LOGGER.info("Successfully fetched tags for bucket")
    else:
        raise Exception("Request to fetch tags failed")
    
    tag_set = resp.get("TagSet")
    if tag_set:
        return tag_set
    else:
        return


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
    
    glue_custom_entity_queue_url = os.environ.get(GLUE_CUSTOM_ENTITY_QUEUE_URL_ENV_VAR)
    if not glue_custom_entity_queue_url:
        raise MissingEnvironmentVariable(
            f"{GLUE_CUSTOM_ENTITY_QUEUE_URL_ENV_VAR} environment variable is required")
    
    bucket_name = valid_event["bucketName"]
    
    s3_client = boto3.client("s3")
    bucket_tags = _get_bucket_tags(s3_client, bucket_name)
    s3_client.close()

    if not bucket_tags:
        LOGGER.warning(f"{bucket_name} does not have any tags. Exiting.")
        return
    
     # prepare payload for glue tracking queue
    message_dict = {}
    message_dict["data_source_type"] = "s3"
    message_dict["data_source_attrs"] = valid_event

    sqs_client = boto3.client("sqs")
    
    tag_match_flag = False
    for tag in bucket_tags:
        if tag["Key"] == VALID_BUCKET_TAG_KEY and tag["Value"] == VALID_BUCKET_TAG_VALUE:
            tag_match_flag = True

        if tag["Key"] == VALID_CUSTOM_ENTITY_TAG_KEY and tag["Value"] == VALID_CUSTOM_ENTITY_TAG_VALUE:
            # send message to Glue custom entity initial queue
            _send_message_to_sqs(
                sqs_client, 
                glue_custom_entity_queue_url, 
                message_dict)


    if not tag_match_flag:
        LOGGER.warning(f"{bucket_name} does not have the required tag. Exiting.")
        return

    else:
        # send message to Glue job tracking Queue
        _send_message_to_sqs(
            sqs_client, 
            glue_queue_url, 
            message_dict)

    sqs_client.close()
    return
