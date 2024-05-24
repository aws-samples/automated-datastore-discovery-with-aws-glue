import json
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError


DB_NAME_ENV_VAR = "DB_NAME"
REGION_ENV_VAR = "AWS_REGION"
DDL_SOURCE_BUCKET_ENV_VAR = "DDL_SOURCE_BUCKET"

LOGGER = logging.getLogger()

DDL_FILE = "rds-ddl.sql"


class MalformedEvent(Exception):
    """Raised if a malformed event received"""
    

class MissingEnvironmentVariable(Exception):
    """Raised if a required environment variable is missing"""


def _check_missing_field(validation_dict, extraction_key):
    """Check if a field exists in a dictionary

    :param validation_dict: Dictionary
    :param extraction_key: String

    :raises: MalformedEvent
    """
    extracted_value = validation_dict.get(extraction_key)
    
    if not extracted_value:
        LOGGER.error(f"Missing '{extraction_key}' key in the dict")
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
        LOGGER.error(f"Incorrect value found for '{extraction_key}' key")
        raise ValueError


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


def _execute_sql(client, secret, dbname, resource_arn, sql):
    """Execute passed in SQL against RDS using the Data API

    :param client: RDS Data Client Object (boto3)
    :param secret: String (ARN of the secret)
    :param dbname: String
    :param resource_arn: String
    :param SQL: String (sql to be executed)

    :raises: Exception
    """
    LOGGER.info(f"Attempting to execute ddl statement: {sql}")
    response = client.execute_statement(
        resourceArn=resource_arn,
        secretArn=secret,
        database=dbname,
        sql=sql,
        )

    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        LOGGER.info("Successfully executed DDL statement")
        return
    else:
        LOGGER.error("Failed to insert record")
        raise Exception
    

def _get_ddl_source_file_contents(client, bucket, filename):
    """Fetch the contents of the DDL SQL file

    :param client: boto3 Client Object (S3)
    :param bucket: String
    :param filename: String

    :raises: Exception

    :rtype String
    """
    resp = client.get_object(Bucket=bucket, Key=filename)

    _check_missing_field(resp, "ResponseMetadata")

    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)

    _check_missing_field(resp, "Body")
    body_obj = resp["Body"]
    
    return body_obj.read().decode("utf-8")


def get_db_cluster_id_from_secret_name(secret_name):
    """Return DB Cluster ID from secret

    :param secret_name: String
    
    :raises: botocore.exceptions.ClientError
    
    :rtype: String
    """
    session = boto3.session.Session()

    # Initializing Secret Manager's client    
    client = session.client(
        service_name='secretsmanager',
            region_name=os.environ.get("AWS_REGION", session.region_name)
        )
    LOGGER.info(f"Attempting to get secret value for: {secret_name}")
    try:
        get_secret_value_response = client.get_secret_value(
                SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        LOGGER.error("Unable to fetch details from Secrets Manager")
        raise e
    
    _check_missing_field(get_secret_value_response, "SecretString")
    
    secret_dict = json.loads(get_secret_value_response["SecretString"])
    
    cluster_id = secret_dict.get("dbClusterIdentifier")
    if not cluster_id:
        LOGGER.warning("Secret does not contain dbClusterIdentifier")
    
    return cluster_id


def _fetch_secret_for_db(cluster_identifier):
    """Fetch the secret arn, name for the database cluster

    :param cluster_identifier: String

    :rtype: String
    """
    arn = ""

    sm_client = boto3.client("secretsmanager")

    resp = sm_client.list_secrets()

    _check_missing_field(resp, "ResponseMetadata")

    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)

    _check_missing_field(resp, "SecretList")

    for secret in resp["SecretList"]:
        _check_missing_field(secret, "Name")
        cluster_id = get_db_cluster_id_from_secret_name(secret["Name"])
        
        if not cluster_id:
            LOGGER.warning("No cluster ID fetched from secret name")
            continue
        
        if cluster_id == cluster_identifier:
            LOGGER.info("Found matching secret for the database")
            _check_missing_field(secret, "ARN")
            arn = secret["ARN"]
            break

    sm_client.close()
    return arn


def lambda_handler(event, context):
    """What executes when the program is run"""
    
    # configure python logger for Lambda
    _configure_logger()
    # silence chatty libraries for better logging
    _silence_noisy_loggers()

    LOGGER.info("Waiting for DDL source to be updated..")
    time.sleep(120)

    ddl_source_file = os.environ.get("DDL_SOURCE_FILE_RDS", DDL_FILE)
    source_s3_bucket = os.environ.get(DDL_SOURCE_BUCKET_ENV_VAR)
    if not source_s3_bucket:
        raise MissingEnvironmentVariable(DDL_SOURCE_BUCKET_ENV_VAR)
    

    db_name = os.environ.get(DB_NAME_ENV_VAR)
    if not db_name:
        raise MissingEnvironmentVariable(DDL_SOURCE_BUCKET_ENV_VAR)
    
    cluster_arn = ""
    cluster_id = source_s3_bucket.replace("ddl-source-", "")
    
    rds_client = boto3.client("rds")
    LOGGER.info("Attempting to get cluster arn from RDS")
    resp = rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)
    rds_client.close()
    
    _check_missing_field(resp, "ResponseMetadata")
    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)

    _check_missing_field(resp, "DBClusters")
    try:
        cluster_details = resp["DBClusters"][0]
        _check_missing_field(cluster_details, "DBClusterArn")
        cluster_arn = cluster_details["DBClusterArn"]
    except IndexError:
        LOGGER.error("No clusters returned from the API call")

    if not cluster_arn:
        LOGGER.warning("Unable to fetch cluster arn from RDS API call."
                       " Will attempt to infer it.")
        region = os.environ.get(REGION_ENV_VAR)
        if not region:
            raise MissingEnvironmentVariable(REGION_ENV_VAR)
        
        account_id = boto3.client('sts').get_caller_identity().get('Account')
        if not account_id:
            LOGGER.warning("Unable to fetch account_id from sts")
        else:
            cluster_arn = f"arn:aws:rds:{region}:{account_id}:cluster:{cluster_id}"
    
    if not cluster_arn:
        LOGGER.error("Unable to find a matching cluster ARN. Exiting.")
        raise Exception
    
    LOGGER.info(f"Cluster ARN: {cluster_arn}")
    
    secret_arn = _fetch_secret_for_db(cluster_id)
    if not secret_arn:
        LOGGER.error(
            f"No matching secret found associated with the cluster: {cluster_id}. Exiting")
        raise Exception
    
    s3_client = boto3.client("s3")
    file_content_string = _get_ddl_source_file_contents(
        s3_client, source_s3_bucket, ddl_source_file)
    s3_client.close()

    rds_data_client = boto3.client("rds-data")

    sql_statements = file_content_string.split(";")
    for sql in sql_statements:
        # get rid of white spaces
        eff_sql = sql.strip(" \n\t")
        if eff_sql:
            _execute_sql(
                rds_data_client, 
                secret_arn,
                db_name,
                cluster_arn,
                eff_sql)

    rds_data_client.close()