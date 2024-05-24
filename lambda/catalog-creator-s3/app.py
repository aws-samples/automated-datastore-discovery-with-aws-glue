import logging
import os

import boto3
from boto3.dynamodb.types import TypeDeserializer


LOGGER = logging.getLogger()

DDB_PARTIQL = "SELECT * FROM {} WHERE glue_job_created = False AND data_catalog_entry = False AND data_source_type = 's3'"

GLUE_TRACKER_DDB_TABLE_NAME_ENV_VAR = "DDB_GLUE_TRACKER_TABLE_NAME"
DATA_CATALOG_DB_NAME_ENV_VAR = "DATA_CATALOG_DB_NAME"

GLUE_ROLE_ARN_ENV_VAR = "GLUE_ROLE_ARN"


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

    :rtype: Dictionary
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


def update_ddb(client, table_name, obj, dc_table_name, dc_db_name):
    """Update the dynamodb entry in the tracker table

    :param client: Boto3 Client Object
    :param table_name: String
    :param obj: Dictionary
    :param dc_table_name: String
    :param dc_db_name: String
    """
    resp = client.update_item(
        TableName=table_name,
        Key={"id": {"S": obj["id"]}},
        UpdateExpression="SET #data_catalog_entry = :true, #data_catalog_table_name =:t, #data_catalog_db_name =:d",
        ExpressionAttributeNames={
            "#data_catalog_entry": "data_catalog_entry",
            "#data_catalog_table_name": "data_catalog_table_name",
            "#data_catalog_db_name": "data_catalog_db_name",
        },
        ExpressionAttributeValues={
            ":true": {"BOOL": True},
            ":t": {"S": dc_table_name},
            ":d": {"S": dc_db_name},
        }
    )
    _check_missing_field(resp, "ResponseMetadata")
    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)
    LOGGER.info("Successfully updated DynamoDB item")


def create_data_catalog_table(client, bucket_name, data_catalog_db_name, account_id, table_name):
    """Create data catalog table for the S3 source

    :param client: Boto3 client obj
    :param bucket_name: String
    :param data_catalog_db_name: String
    :param account_id: String
    :param table_name: String
    """
    LOGGER.info(f"Attempting to create table: {table_name} in DB: {data_catalog_db_name}")
    resp = client.create_table(
        CatalogId=account_id,
        DatabaseName=data_catalog_db_name,
        TableInput={
            "Name": table_name,
            "StorageDescriptor": {
                "Location": f"s3://{bucket_name}/"
            }
        }
    )
    _check_missing_field(resp, "ResponseMetadata")
    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)


def create_crawler(client, data_catalog_db_name, table_name, role_arn):
    """Create crawler for the S3 source

    :param client: Boto3 client obj
    :param data_catalog_db_name: String
    :param table_name: String
    :param role_arn: String
    """
    crawler_name = f"{table_name}_crawler"
    LOGGER.info(f"Attempting to create crawler: {table_name}")
    resp = client.create_crawler(
        Name=crawler_name,
        Role=role_arn,
        DatabaseName=data_catalog_db_name,
        Targets={
        'CatalogTargets': [
            {
                'DatabaseName': data_catalog_db_name,
                'Tables': [
                    table_name,
                ],
            },
        ],
        },
         SchemaChangePolicy={
        'UpdateBehavior': 'UPDATE_IN_DATABASE',
        'DeleteBehavior': 'LOG'},
        Schedule='cron(0 2 * * ? *)',
        )
    _check_missing_field(resp, "ResponseMetadata")
    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)


def lambda_handler(event, context):
    """What executes when the program is run"""
    
    # configure python logger
    _configure_logger()
    # silence chatty libraries
    _silence_noisy_loggers()

    ddb_table_name = os.environ.get(GLUE_TRACKER_DDB_TABLE_NAME_ENV_VAR)
    if not ddb_table_name:
        raise MissingEnvironmentVariable(f"{GLUE_TRACKER_DDB_TABLE_NAME_ENV_VAR} is missing")
    
    ddb_client = boto3.client("dynamodb")

    ddb_resp = _fetch_ddb_results(
        ddb_client, 
        DDB_PARTIQL.format(ddb_table_name),
    )

    ddb_results = ddb_resp.get("Items")
    if not ddb_results:
        LOGGER.warning("No data sources returned. Exiting.")
        return

    pythonic_results = unmarshall_ddb_items(ddb_results)
    
    role_arn = os.environ.get(GLUE_ROLE_ARN_ENV_VAR)
    if not role_arn:
        raise MissingEnvironmentVariable(f"{GLUE_ROLE_ARN_ENV_VAR} is missing")
    
    data_catalog_db_name = os.environ.get(DATA_CATALOG_DB_NAME_ENV_VAR)
    if not data_catalog_db_name:
        raise MissingEnvironmentVariable(f"{DATA_CATALOG_DB_NAME_ENV_VAR} is missing")
    
    # need account id for catalog table creation
    account_id = boto3.client("sts").get_caller_identity()["Account"]
 
    glue_client = boto3.client("glue")

    for s3_obj in pythonic_results:
        bucket_name = s3_obj["data_source_attrs"]["bucketName"]
        # glue data catalog only likes _ as special characters
        catalog_bucket_name = bucket_name.replace("-", "_")
        catalog_bucket_name = catalog_bucket_name.replace(".", "_")
        dc_table_name = f"{data_catalog_db_name}_{catalog_bucket_name}"
        
        create_data_catalog_table(
            client=glue_client,
            bucket_name=bucket_name,
            data_catalog_db_name=data_catalog_db_name,
            account_id=account_id,
            table_name=dc_table_name

        )
        LOGGER.info("Successfully created data catalog table")

        create_crawler(
            client=glue_client,
            data_catalog_db_name=data_catalog_db_name,
            table_name=dc_table_name,
            role_arn=role_arn
        )
        LOGGER.info("Successfully created crawler")
        
        LOGGER.info("Attempting to update glue job tracker table")
        update_ddb(
                client=ddb_client,
                obj=s3_obj,
                table_name=ddb_table_name,
                dc_table_name=dc_table_name,
                dc_db_name=data_catalog_db_name
            )


    LOGGER.debug("Closing DynamoDB Boto3 client") 
    ddb_client.close()

    LOGGER.debug("Closing Glue boto3 client")
    glue_client.close()
