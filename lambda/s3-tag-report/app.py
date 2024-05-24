import datetime
import logging
import os
import uuid

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError


LOGGER = logging.getLogger()

DDB_PARTIQL = "SELECT * FROM {} WHERE data_catalog_entry = True AND data_source_type = 's3'"

GLUE_TRACKER_DDB_TABLE_NAME_ENV_VAR = "DDB_GLUE_TRACKER_TABLE_NAME"
DDB_TAG_TABLE_NAME_ENV_VAR = "TAG_REPORT_TABLE_NAME"

REQUIRED_TAG_KEYS = ["APP_ID", "Purpose of Processing", "Data Subjects", "APP_NAME", "Business Function", "Processing Activity"]


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
    

def _fetch_ddb_results(client, query):
    """Fetch results from PartiQL DynamoDB query

    :param client: Boto3 client (DynamoDB)
    :param query: String

    :rtype: List
    """
    resp = client.execute_statement(Statement=query)

    _check_missing_field(resp, "ResponseMetadata")

    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)

    return resp


def unmarshall_ddb_items(ddb_items):
    """Deserialize ddb_items

    :param ddb_items: List

    :rtype: List
    """
    unmarshalled = []

    deserializer = TypeDeserializer()

    for ddb_item in ddb_items:
        unmarshalled.append(
            {k: deserializer.deserialize(v) for k, v in ddb_item.items()}
        )
    
    return unmarshalled


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

    ddb_table_name = os.environ.get(GLUE_TRACKER_DDB_TABLE_NAME_ENV_VAR)
    if not ddb_table_name:
        raise MissingEnvironmentVariable(f"{GLUE_TRACKER_DDB_TABLE_NAME_ENV_VAR} is missing")
    
    tag_table_name = os.environ.get(DDB_TAG_TABLE_NAME_ENV_VAR)
    if not tag_table_name:
        raise MissingEnvironmentVariable(f"{DDB_TAG_TABLE_NAME_ENV_VAR} is missing")
    
    ddb_client = boto3.client("dynamodb")

    ddb_resp = _fetch_ddb_results(
        ddb_client, 
        DDB_PARTIQL.format(ddb_table_name),
    )

    ddb_results = ddb_resp.get("Items")
    if not ddb_results:
        LOGGER.warning("No data sources fetched. Exiting.")
        return

    pythonic_results = unmarshall_ddb_items(ddb_results) 

    s3_client = boto3.client("s3")
    for obj in pythonic_results:
        bucket_name = obj["data_source_attrs"]["bucketName"]
        LOGGER.info(f"S3 bucket name: {bucket_name}")
        
        bucket_tags = _get_bucket_tags(s3_client, bucket_name)
        if not bucket_tags:
            LOGGER.error(f"{bucket_name} does not have any tags. Skipping.")
            continue
        
        data_catalog_table_name = obj["data_catalog_table_name"]
        LOGGER.info(f"Data catalog table name: {data_catalog_table_name}")
        
        tag_obj = {}
        
        for tag in bucket_tags:
            if tag["Key"] in REQUIRED_TAG_KEYS:
                tag_obj[tag["Key"]] = tag["Value"]

        if not tag_obj:
            LOGGER.error("None of the required tags are present. Skipping.")
            continue
        else:
            LOGGER.info("Attempting to update tag reporting table")
            tag_obj["id"] = str(uuid.uuid4())
            tag_obj["data_catalog_table_name"] = data_catalog_table_name
            tag_obj["time_stamp"] = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
            serializer = TypeSerializer()
            resp = ddb_client.put_item(
                TableName=tag_table_name,
                Item={
                    k: serializer.serialize(v) for k, v in tag_obj.items()
                    },
            )
            _check_missing_field(resp, "ResponseMetadata")
            _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)

    LOGGER.debug("Closing S3 Boto3 client") 
    s3_client.close()

    LOGGER.debug("Closing DynamoDB Boto3 client") 
    ddb_client.close()
