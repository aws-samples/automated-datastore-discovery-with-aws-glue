import argparse
import logging
import os
import random
import time

import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError
from faker import Faker


DEFAULT_LOG_LEVEL = logging.INFO
LOGGER = logging.getLogger(__name__)
LOGGING_FORMAT = "%(asctime)s %(levelname)-5.5s " \
                 "[%(name)s]:[%(threadName)s] " \
                 "%(message)s"

AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN"

TABLE_NAME = "NetworkInfo"

MAX_RECORDS = 1000


def _check_missing_field(validation_dict, extraction_key):
    """Check if a field exists in a dictionary

    :param validation_dict: Dictionary
    :param extraction_key: String

    :raises: Exception
    """
    extracted_value = validation_dict.get(extraction_key)
    
    if extracted_value is None:
        LOGGER.error(f"Missing '{extraction_key}' key in the dict")
        raise Exception
    

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
        LOGGER.error(f"Incorrect value found for '{extraction_key}' key")
        raise ValueError


def _cli_args():
    """Parse CLI Args
    
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(description="synthetic-data-gen-ddb")
    parser.add_argument("-e",
                        "--env",
                        action="store_true",
                        help="Use environment variables for AWS credentials")
    parser.add_argument("-p",
                        "--aws-profile",
                        type=str,
                        default="default",
                        help="AWS profile to be used for the API calls")
    parser.add_argument("-r",
                        "--aws-region",
                        type=str,
                        help="AWS region for API calls")
    parser.add_argument("-m",
                        "--max-records",
                        type=int,
                        default=MAX_RECORDS,
                        help="Maximum records to be inserted")
    parser.add_argument("-v",
                        "--verbose",
                        action="store_true",
                        help="debug log output")
    return parser.parse_args()


def _silence_noisy_loggers():
    """Silence chatty libraries for better logging"""
    for logger in ['boto3', 'botocore',
                   'botocore.vendored.requests.packages.urllib3']:
        logging.getLogger(logger).setLevel(logging.WARNING)


def main():
    """What executes when the script is run"""
    start = time.time() # to capture elapsed time

    args = _cli_args()

    # logging configuration
    log_level = DEFAULT_LOG_LEVEL
    if args.verbose:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)
    # silence chatty libraries
    _silence_noisy_loggers()

    if args.env:
        LOGGER.info(
            "Attempting to fetch AWS credentials via environment variables")
        aws_access_key_id = os.environ.get(AWS_ACCESS_KEY_ID)
        aws_secret_access_key = os.environ.get(AWS_SECRET_ACCESS_KEY)
        aws_session_token = os.environ.get(AWS_SESSION_TOKEN)
        if not aws_secret_access_key or not aws_access_key_id or not aws_session_token:
            raise Exception(
                f"Missing one or more environment variables - " 
                f"'{AWS_ACCESS_KEY_ID}', '{AWS_SECRET_ACCESS_KEY}', "
                f"'{AWS_SESSION_TOKEN}'"
            )
    else:
        LOGGER.info(f"AWS Profile being used: {args.aws_profile}")
        boto3.setup_default_session(profile_name=args.aws_profile)

    region_check = [
        # explicit cli argument
        args.aws_region,
        # check if set through ENV vars
        os.environ.get('AWS_REGION'),
        os.environ.get('AWS_DEFAULT_REGION'),
        # else check if set in config or in boto already
        boto3.DEFAULT_SESSION.region_name if boto3.DEFAULT_SESSION else None,
        boto3.Session().region_name,
    ]
    aws_region = None
    for aws_region in region_check:
        if aws_region:
            LOGGER.info(f"AWS Region: {aws_region}")
            break
    if not aws_region:
        raise Exception("Need to have a valid AWS Region")

    ddb_client = boto3.client("dynamodb")
    table_name = os.environ.get(
        "NETWORK_TABLE_NAME", TABLE_NAME)
    # Check if table exists
    list_table_resp = ddb_client.list_tables()
    _check_missing_field(list_table_resp, "ResponseMetadata")
    _validate_field(
        list_table_resp["ResponseMetadata"], 
        "HTTPStatusCode", 
        200)
    _check_missing_field(list_table_resp, "TableNames")
    
    tables = list_table_resp["TableNames"]
    if table_name not in tables:
        raise Exception(f"Table {table_name} does not exist")

    serializer = TypeSerializer()
    fake = Faker()
    # Generate and insert data into the table
    for _ in range(MAX_RECORDS):
        python_obj = {
            'IP_Address_IPv4_Individually_Identifiable': fake.ipv4(),
            'IP_Address_IPv6_Individually_Identifiable': fake.ipv6(),
            'IP_Address_Non_Individually_Identifiable': fake.ipv4_private(),
            'MAC_Address': fake.mac_address(),
            'id': str(random.randint(1000000000, 9999999999))  # Random 10-digit number
        }
        try:
            resp = ddb_client.put_item(
                TableName=table_name,
                Item={
                    k: serializer.serialize(v) for k, v in python_obj.items()
                    },
                ConditionExpression="attribute_not_exists(id)",
            )
            _check_missing_field(resp, "ResponseMetadata")
            _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)       
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                LOGGER.error(f"Primary Key violation")
            else:
                LOGGER.error("Unable to insert data into DynamoDB")
                raise Exception

        LOGGER.info(f"Inserted item with id: {python_obj['id']}.")

    LOGGER.info(f"Data insertion complete for table: {table_name}.")

    ddb_client.close()
    LOGGER.info(f"Total time elapsed: {time.time() - start} seconds")


if __name__ == "__main__":
    main()

