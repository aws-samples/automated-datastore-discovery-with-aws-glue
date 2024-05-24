import copy
import datetime
import logging
import os

import boto3
from boto3.dynamodb.types import TypeDeserializer


LOGGER = logging.getLogger()

PARTIQL = "SELECT * FROM {} WHERE data_source_type = '{}'"

PII_PARTIQL = """
SELECT columnName, entityTypes FROM {} WHERE data_catalog_table='{}' AND "timestamp"='{}'
"""

DATE_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

LOGGER = logging.getLogger()

DELETE_PROPS = ['CreateTime', 'UpdateTime',  'CreatedBy', 'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId', 'DatabaseName']

PII_OUTPUT_TABLE = "OUTPUT_TABLE_NAME"
DATA_SOURCE = "DATA_SOURCE_NAME"


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


def glue_get_table(client, table_name, account_id, database_name):
    """Execute PARTIQL statement against DynamoDB
    
    :param client: Boto3 client object
    :param table_name: String
    :param account_id: String
    :param database_name: String
    
    :rtype: Dictionary
    """
    resp = client.get_table(
        CatalogId=account_id,
        DatabaseName=database_name,
        Name=table_name,
        ) 
    _check_missing_field(resp, "ResponseMetadata")

    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)
    
    return resp


def update_catalog_table(client, table_name, database_name, account_id, columns):
    """Update catalog table column comments for sensitive data
    
    :param client: Boto3 client object
    :param table_name: String
    :param database_name: String
    :param account_id: String
    :param columns: List
    
    :raises Exception
    """
    pii_col_dict = {}
    
    for col in columns:
        pii_col_dict[col["columnName"].lower()] = str(col["entityTypes"])
            
    table_resp = glue_get_table(client, table_name, account_id, database_name)
    
    table_dict = table_resp.get("Table")
    
    if not table_dict:
        LOGGER.error("No valid table returned.")
        raise Exception
        
    update_table_dict = copy.deepcopy(table_dict)

    version = update_table_dict["VersionId"]
    
    for del_prop in DELETE_PROPS:
        del update_table_dict[del_prop]
    
    catalog_cols = copy.deepcopy(update_table_dict["StorageDescriptor"]["Columns"])

    update_cols = []
    pii_col_list = list(pii_col_dict.keys())

    for col_obj in catalog_cols:
        col_name = col_obj["Name"]
        if col_name in pii_col_list:
            comment_str = f"Sensitive Data Element | {pii_col_dict[col_name]}"
            if len(comment_str) > 255:
                LOGGER.warning(f"Comment string '{comment_str}' is longer than 255 characters. Will trim it")
                comment_str = comment_str[:255]
            col_obj["Comment"] = comment_str
            
        update_cols.append(col_obj)
            
    update_table_dict["StorageDescriptor"]["Columns"] = update_cols
    
    resp = client.update_table(
        CatalogId=account_id,
        DatabaseName=database_name,
        TableInput=update_table_dict,
        VersionId=version
        )
        
    _check_missing_field(resp, "ResponseMetadata")

    _validate_field(resp["ResponseMetadata"], "HTTPStatusCode", 200)
    
    LOGGER.info("Successfully update Glue Data Catalog Table")
    

def lambda_handler(event, context):
    """What executes when the program is run"""
    
    # configure python logger
    _configure_logger()
    # silence chatty libraries
    _silence_noisy_loggers()

    ddb_table_name = os.environ.get(PII_OUTPUT_TABLE)
    if not ddb_table_name:
        raise MissingEnvironmentVariable(f"{PII_OUTPUT_TABLE} is missing")
    
    data_source = os.environ.get(DATA_SOURCE)
    if not data_source:
        raise MissingEnvironmentVariable(f"{DATA_SOURCE} is missing")
    
    ddb_client = boto3.client("dynamodb")

    ddb_resp = _fetch_ddb_results(
        ddb_client, 
        PARTIQL.format(ddb_table_name, data_source),
    )

    ddb_results = ddb_resp.get("Items")
    if not ddb_results:
        LOGGER.warning("No results fetched. Exiting.")
        return

    pythonic_results = unmarshall_ddb_items(ddb_results)

    catalog_tables_dict = {}  
    # fetching latest timestamp per catalog table
    for rec in pythonic_results:
        keys = catalog_tables_dict.keys()
        catalog_table = rec["data_catalog_table"]
        time_str = rec["timestamp"]
        timestamp = datetime.datetime.strptime(time_str, DATE_FORMAT)
        
        if catalog_table not in keys:
            obj = {
                "catalog_database_name": rec["data_catalog_database"],
                "timestamp": timestamp,
                "time_str": time_str
            }
            catalog_tables_dict[catalog_table] = obj
        else:
            existing_obj = catalog_tables_dict[catalog_table]
            if timestamp > existing_obj["timestamp"]:
                catalog_tables_dict[catalog_table]["timestamp"] = timestamp
                catalog_tables_dict[catalog_table]["time_str"] = time_str
    
    # need account id for glue API calls
    account_id = boto3.client("sts").get_caller_identity()["Account"]
 
    glue_client = boto3.client("glue")

    for table in catalog_tables_dict.keys():
        
        LOGGER.info(f"Fetching PII elements from table: {table}")
        
        pii_results = _fetch_ddb_results(
            ddb_client, 
            PII_PARTIQL.format(
                ddb_table_name, 
                table, 
                catalog_tables_dict[table]["time_str"])
            )
            
        pii_items = pii_results.get("Items")
        pythonic_pii_items = unmarshall_ddb_items(pii_items)
                
        update_catalog_table(
            glue_client, 
            table, 
            catalog_tables_dict[table]["catalog_database_name"], 
            account_id, 
            pythonic_pii_items
        )

    LOGGER.debug("Closing Glue Boto3 client") 
    glue_client.close()

    LOGGER.debug("Closing DynamoDB Boto3 client") 
    ddb_client.close()
