import copy
import logging
import os

import boto3
from boto3.dynamodb.types import TypeDeserializer


LOGGER = logging.getLogger()

GLUE_DEFAULT_ARGS = {
            "--enable-metrics": "true",
            "--enable-spark-ui": "true",
            "--enable-job-insights": "false",
            "--enable-glue-datacatalog": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--job-bookmark-option": "job-bookmark-disable",
            "--job-language": "python",
        }

DDB_PARTIQL = "SELECT * FROM {} WHERE glue_job_created = False AND data_catalog_entry = True AND data_source_type = 's3'"

GLUE_TRACKER_DDB_TABLE_NAME_ENV_VAR = "DDB_GLUE_TRACKER_TABLE_NAME"

PII_OUTPUT_DDB_TABLE_NAME_ENV_VAR = "PII_OUTPUT_TABLE_NAME"

GLUE_SCRIPT_ENV_VAR = "GLUE_SCRIPT_BUCKET"
GLUE_ASSETS_ENV_VAR = "GLUE_ASSETS_BUCKET"

GLUE_ROLE_ARN_ENV_VAR = "GLUE_ROLE_ARN"

GLUE_SCRIPT_FILE_NAME = "s3-source-script.py"

GLUE_JOB_PYTHON_VERSION = "3"
GLUE_JOB_TYPE_NAME = "glueetl"
GLUE_JOB_MAX_RETRIES = 0
GLUE_JOB_TIMEOUT = 2880
GLUE_JOB_WORKER_TYPE = "G.1X"
GLUE_JOB_NUM_WORKERS = 10
GLUE_JOB_VERSION = "4.0"
GLUE_JOB_EXEC_CLASS = "STANDARD"
GLUE_JOB_MAX_CONCURRENT_RUNS = 1


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


def update_ddb(client, table_name, obj):
    """Update the dynamodb entry in the tracker table

    :param client: Boto3 Client Object
    :param table_name: String
    :param obj: Dictionary
    """
    resp = client.update_item(
        TableName=table_name,
        Key={"id": {"S": obj["id"]}},
        UpdateExpression="SET #glue_job_created = :true",
        ExpressionAttributeNames={
            "#glue_job_created": "glue_job_created",
        },
        ExpressionAttributeValues={
            ":true": {"BOOL": True},
        }
    )
    _check_missing_field(resp, "ResponseMetadata")
    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)
    LOGGER.info("Successfully updated DynamoDB item")


def create_glue_job(
        client, 
        script_location, 
        role_arn, 
        spark_logs_path, 
        temp_dir, 
        bucket_name,
        dc_table_name, 
        output_table,
        dc_db_name,
        s3_host,
        region,
        glue_job_name
        ):
    """Create a glue job for a data source

    :param client: Boto3 client object (Glue)
    :param script_location: String
    :param role_arn: String
    :param spark_logs_path: String
    :param temp_dir: String
    :param bucket_name: String
    :param dc_table_name: String
    :param output_table: String
    :param dc_db_name: String
    :param s3_host: String
    :param region: String
    :param glue_job_name: String
    """
    glue_args = copy.deepcopy(GLUE_DEFAULT_ARGS)

    glue_args["--spark-event-logs-path"] = spark_logs_path
    glue_args["--TempDir"] = temp_dir
    glue_args["--s3Bucket"] = bucket_name
    glue_args["--dataCatalogTable"] = dc_table_name
    glue_args["--dataCatalogDatabase"] = dc_db_name
    glue_args["--outputTable"] = output_table
    glue_args["--s3Host"] = s3_host
    glue_args["--region"] = region

    resp = client.create_job(
        Name=glue_job_name,
        Command={
            "Name": GLUE_JOB_TYPE_NAME,
            "PythonVersion": GLUE_JOB_PYTHON_VERSION,
            "ScriptLocation": script_location
        },
        Role=role_arn,
        DefaultArguments=glue_args,
        MaxRetries=GLUE_JOB_MAX_RETRIES,
        Timeout=GLUE_JOB_TIMEOUT,
        WorkerType=GLUE_JOB_WORKER_TYPE,
        NumberOfWorkers=GLUE_JOB_NUM_WORKERS,
        GlueVersion=GLUE_JOB_VERSION,
        ExecutionClass=GLUE_JOB_EXEC_CLASS,
        ExecutionProperty={
            "MaxConcurrentRuns": GLUE_JOB_MAX_CONCURRENT_RUNS
            }
    )
    _check_missing_field(resp, "ResponseMetadata")

    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)

    LOGGER.info(f"Successfully created glue job: {glue_job_name}")

   
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
        LOGGER.warning("No data sources fetched. Exiting.")
        return

    pythonic_results = unmarshall_ddb_items(ddb_results)

    output_table = os.environ.get(PII_OUTPUT_DDB_TABLE_NAME_ENV_VAR)
    if not output_table:
        raise MissingEnvironmentVariable(f"{PII_OUTPUT_DDB_TABLE_NAME_ENV_VAR} is missing")

    assets_bucket = os.environ.get(GLUE_ASSETS_ENV_VAR)
    if not assets_bucket:
        raise MissingEnvironmentVariable(f"{GLUE_ASSETS_ENV_VAR} is missing")
    
    script_bucket = os.environ.get(GLUE_SCRIPT_ENV_VAR)
    if not assets_bucket:
        raise MissingEnvironmentVariable(f"{GLUE_SCRIPT_ENV_VAR} is missing")
    
    role_arn = os.environ.get(GLUE_ROLE_ARN_ENV_VAR)
    if not role_arn:
        raise MissingEnvironmentVariable(f"{GLUE_ROLE_ARN_ENV_VAR} is missing")
    
    glue_client = boto3.client("glue")

    for s3_obj in pythonic_results:
        bucket_name = s3_obj["data_source_attrs"]["bucketName"]
        LOGGER.info(f"Attempting to create glue job for s3 source: {bucket_name}")
        
        data_catalog_table_name = s3_obj["data_catalog_table_name"]
        LOGGER.info(f"Glue Data Catalog Table Name: {data_catalog_table_name}")
        
        data_catalog_db_name = s3_obj["data_catalog_db_name"]
        LOGGER.info(f"Glue Data Catalog Database Name: {data_catalog_db_name}")
        try:
            region = s3_obj["data_source_attrs"]["CreateBucketConfiguration"]["LocationConstraint"]
        except KeyError:
            LOGGER.warning("'CreateBucketConfiguration' not found in data source attrs")
            region = os.environ.get("AWS_REGION", "noregion")
        glue_job_name = f"s3-pii-detect-{region}-{data_catalog_table_name}"
        create_glue_job(
            client=glue_client,
            script_location=f"s3://{script_bucket}/{GLUE_SCRIPT_FILE_NAME}",
            role_arn=role_arn,
            spark_logs_path=f"s3://{assets_bucket}/sparkHistoryLogs/",
            temp_dir=f"s3://{assets_bucket}/temporary/",
            bucket_name=bucket_name,
            dc_table_name=data_catalog_table_name,
            output_table=output_table,
            dc_db_name=data_catalog_db_name,
            s3_host=s3_obj["data_source_attrs"]["Host"],
            region=region,
            glue_job_name=glue_job_name
        )
        
        wf_name = f"s3-wf-{region}-{data_catalog_table_name}"
        wf_resp = glue_client.create_workflow(
            Name=wf_name
        )
        _check_missing_field(wf_resp, "ResponseMetadata")
        _validate_field(wf_resp["ResponseMetadata"], "HTTPStatusCode", 200)
        LOGGER.info(f"Successfully created glue workflow: {wf_name}")

        trigger_resp = glue_client.create_trigger(
            Name=f"s3-glue-trigger-{region}-{data_catalog_table_name}",
            WorkflowName=wf_name,
            StartOnCreation=True,
            Type="SCHEDULED",
            Schedule='cron(0 6 ? * MON-FRI *)',
            Actions=[
                {
                    "JobName": glue_job_name
                }
            ]
        )
        _check_missing_field(trigger_resp, "ResponseMetadata")
        _validate_field(trigger_resp["ResponseMetadata"], "HTTPStatusCode", 200)
        LOGGER.info(f"Successfully created glue trigger")

        LOGGER.info("Attempting to update glue job tracker table")
        update_ddb(
            client=ddb_client,
            obj=s3_obj,
            table_name=ddb_table_name
            )

    LOGGER.debug("Closing DynamoDB Boto3 client") 
    ddb_client.close()

    LOGGER.debug("Closing Glue boto3 client")
    glue_client.close()
