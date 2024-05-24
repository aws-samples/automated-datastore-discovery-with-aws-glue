import copy
import json
import logging
import os

import boto3
from botocore.exceptions import ClientError


SQS_QUEUE_URL_ENV_VAR = "SQS_QUEUE_URL"
SEC_GROUP_ENV_VAR = "CONN_SEC_GROUP_ID"
CRAWLER_SCHEMAS_EXC_ENV_VAR = "CRAWLER_SCHEMAS_EXCEPTION"
CATALOG_DB_NAME_ENV_VAR = "CATALOG_DB_NAME"
CRAWLER_ROLE_ENV_VAR = "CRAWLER_ROLE_ARN"

DEFAULT_EXCEPTION_SCHEMAS = [
    "pg_catalog",
    "information_schema"
]

BASE_SCHEMA_SQL = """
SELECT DISTINCT(table_schema) FROM information_schema.tables WHERE table_schema NOT IN {}
"""

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


def _get_db_cluster_identifier(secret_name):
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
    
    try:
        secret_dict = json.loads(get_secret_value_response["SecretString"])
        return secret_dict.get("dbClusterIdentifier")
    except json.decoder.JSONDecodeError:
        LOGGER.warning("Found a non kv secret")
        return
    

def _fetch_secret_for_db(cluster_identifier):
    """Fetch the secret arn, name for the database cluster

    :param cluster_identifier: String

    :rtype: Tuple<String, String>
    """
    arn = ""
    name = ""

    sm_client = boto3.client("secretsmanager")

    resp = sm_client.list_secrets()

    _check_missing_field(resp, "ResponseMetadata")

    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)

    _check_missing_field(resp, "SecretList")

    for secret in resp["SecretList"]:
        _check_missing_field(secret, "Name")
        name = secret["Name"]

        cluster_id = _get_db_cluster_identifier(name)
        
        if cluster_id is not None:
            if cluster_id == cluster_identifier:
                LOGGER.info("Found matching secret for the database")
                _check_missing_field(secret, "ARN")
                arn = secret["ARN"]

    sm_client.close()
    return arn, name


def create_glue_conn(client, 
                     connection_name, 
                     protocol, 
                     endpoint, 
                     port, 
                     dbname, 
                     connection_ssl, 
                     secret_name, 
                     account_id,
                     subnet,
                     sec_group_id
                     ):
    """Create glue connection object
    
    :param client: Boto3 client object (Glue)
    :param connection_name: String
    :param protocol: String
    :param endpoint: String
    :param port: Integer
    :param dbname: String
    :param connection_ssl: String
    :param secret_name: String
    :param account_id: String
    :param subnet: Dictionary,
    :param sec_group_id: String
    """
    resp = client.create_connection(
        CatalogId=account_id,
        ConnectionInput={
            "Name": connection_name,
            "ConnectionType": "JDBC",
            "ConnectionProperties": {
                "JDBC_CONNECTION_URL": f"jdbc:{protocol}://{endpoint}:{str(port)}/{dbname}",
                "SECRET_ID": secret_name,
                "JDBC_ENFORCE_SSL": "false" if not connection_ssl else str(connection_ssl).lower()
            },
            "PhysicalConnectionRequirements": {
                "AvailabilityZone": subnet["SubnetAvailabilityZone"]["Name"],
                "SubnetId": subnet["SubnetIdentifier"],
                "SecurityGroupIdList": [sec_group_id],
            }
        }
        )
    _check_missing_field(resp, "ResponseMetadata")
    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)


def get_subnet_for_conn(subnet_group):
    """Fetch a subnet for the connection
    
    :param subnet_group: String
    
    :rtype: Dictionary
    """
    rds_client = boto3.client("rds")
    resp = rds_client.describe_db_subnet_groups(DBSubnetGroupName=subnet_group)
    rds_client.close()

    _check_missing_field(resp, "ResponseMetadata")
    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)
    
    _check_missing_field(resp, "DBSubnetGroups")
    subnet_group_details = resp["DBSubnetGroups"][0]
    
    _check_missing_field(subnet_group_details, "Subnets")
    subnets = subnet_group_details["Subnets"]
    
    return subnets[0]


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


def _execute_sql(client, secret, dbname, resource_arn, sql):
    """Execute passed in SQL against RDS using the Data API

    :param client: RDS Data Client Object (boto3)
    :param secret: String (ARN of the secret)
    :param dbname: String
    :param resource_arn: String
    :param SQL: String (sql to be executed)

    :raises: Exception

    :rtype: Dictionary
    """
    LOGGER.info(f"Attempting to execute sql statement: {sql}")
    response = client.execute_statement(
        resourceArn=resource_arn,
        secretArn=secret,
        database=dbname,
        sql=sql,
        )
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        LOGGER.info("Successfully executed SQL statement")
        return response
    else:
        LOGGER.error("Failed to insert record")
        raise Exception


def lambda_handler(event, context):
    """What executes when the program is run"""
    
    # configure python logger
    _configure_logger()
    # silence chatty libraries
    _silence_noisy_loggers()

    msg_attr = _get_sqs_message_attributes(event)
    
    if msg_attr:
        # Because messages remain in the queue
        LOGGER.info(f"Deleting message {msg_attr['message_id']} from sqs")
        queue_url = os.environ.get(SQS_QUEUE_URL_ENV_VAR)
        if not queue_url:
            raise MissingEnvironmentVariable(
                f"{SQS_QUEUE_URL_ENV_VAR} environment variable is required")
        
        sqs_client = boto3.client("sqs")
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

    body = _get_message_body(event)

    _check_missing_field(body, "dBClusterIdentifier")
    cluster_id = body["dBClusterIdentifier"]
    LOGGER.info(f"cluster id: {cluster_id}")

    _check_missing_field(body, "dBClusterArn")
    cluster_arn = body["dBClusterArn"]
    LOGGER.info(f"cluster ARN: {cluster_arn}")

    _check_missing_field(body, "endpoint")
    endpoint = body['endpoint']
    
    _check_missing_field(body, "engine")
    engine = body['engine']
    
    _check_missing_field(body, "port")
    port = body['port']
    
    # TODO: make more robust
    dbname = body.get("databaseName")
    if not dbname:
        # Other database engines may have something different
        # this will need some more thought to make it more resilient
        dbname = "information_schema"
    
    # TODO: make it more robust
    if "postgres" in engine:
        protocol = "postgresql"
    else:
        protocol = "mysql"

    secret_arn, secret_name = _fetch_secret_for_db(cluster_id)

    if not secret_arn:
        LOGGER.error(
            f"No matching secret found associated with the cluster: {cluster_id}. Exiting")
        return
    
    sec_group_id = os.environ.get(SEC_GROUP_ENV_VAR)
    if not sec_group_id:
        raise MissingEnvironmentVariable(
            f"{SEC_GROUP_ENV_VAR} environment variable is required")
    
    _check_missing_field(body, "dBSubnetGroup")
    subnet_group = body['dBSubnetGroup']
    LOGGER.info(f"Subnet Group Name: {subnet_group}")
    
    subnet = get_subnet_for_conn(subnet_group)
    LOGGER.info(f"Subnet ID: {subnet['SubnetIdentifier']}")
    
    LOGGER.info("fetching account id via API call")
    account_id = boto3.client("sts").get_caller_identity()["Account"]

    connection_name =  f"glue-connection-{cluster_id}"
    glue_client = boto3.client("glue")
    LOGGER.info(f"Creating glue connection object: {connection_name}")
    create_glue_conn(
        client=glue_client,
        connection_name=connection_name,
        protocol=protocol,
        endpoint=endpoint,
        port=port,
        dbname=dbname,
        connection_ssl=os.environ.get("CONNECTION_SSL"),
        secret_name=secret_name,
        account_id=account_id,
        subnet=subnet,
        sec_group_id=sec_group_id
        )

    exc_schemas = copy.deepcopy(DEFAULT_EXCEPTION_SCHEMAS)
    crawler_schemas_exc = os.environ.get(CRAWLER_SCHEMAS_EXC_ENV_VAR)
    if crawler_schemas_exc is not None:
        for exc_schema in crawler_schemas_exc.split(","):
            exc_schemas.append(exc_schema)

    rds_data_client = boto3.client("rds-data")

    sql_resp = _execute_sql(
        rds_data_client, 
        secret_arn,
        dbname,
        cluster_arn,
        BASE_SCHEMA_SQL.format(tuple(exc_schemas))
        )
    rds_data_client.close()
    
    crawler_schemas = []
    recs = sql_resp.get("records", [])
    for rec in recs:
        try:
            rec_first = rec[0]
            schema = rec_first["stringValue"]
            crawler_schemas.append(schema)
        except IndexError:
            LOGGER.error("Invalid record found")
            continue
        except KeyError:
            LOGGER.error("Invalid data type found")
            continue

    LOGGER.info(f"Crawler schemas: {crawler_schemas}")
        
    catalog_db_name = os.environ.get(CATALOG_DB_NAME_ENV_VAR)
    if not catalog_db_name:
        raise MissingEnvironmentVariable(
            f"{CATALOG_DB_NAME_ENV_VAR} environment variable is required")
    
    crawler_role = os.environ.get(CRAWLER_ROLE_ENV_VAR)
    if not crawler_role:
        raise MissingEnvironmentVariable(
            f"{CRAWLER_ROLE_ENV_VAR} environment variable is required")
    
    for schema in crawler_schemas:
        crawler_name = f"glue-crawler-{cluster_id}-{schema}"
        LOGGER.info(f"Creating crawler: {crawler_name}")
        resp = glue_client.create_crawler(
            Name=crawler_name,
            Role=crawler_role,
            DatabaseName=catalog_db_name,
            Targets={
            "JdbcTargets": [
                {
                    "ConnectionName": connection_name,
                    "Path": f"postgres/{schema}/%",
                },],
                },
            Schedule='cron(0 2 * * ? *)',
            )
        _check_missing_field(resp, "ResponseMetadata")
        _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)
        
    glue_client.close()    
