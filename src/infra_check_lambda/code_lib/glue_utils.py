"""Module for holding Glue related methods."""
from email.message import Message
import logging
import os
import copy
import boto3
from botocore.exceptions import ClientError

from .decorators import log_methods_non_sensitive

sns_client = boto3.client("sns")

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


@log_methods_non_sensitive
def table_exists(db_name: str, tb_name: str, region_name: str) -> bool:
    """Check if the table exists in the given region.

    Args:
        db_name (str): of database name in AWS Glue
        tb_name (str): of table exists in given database
        region_name (str): of the region in the AWS Glue database

    Returns:
        bool
    """
    try:
        client = boto3.client('glue', region_name=region_name)
        response_get_table = client.get_table(DatabaseName=db_name, Name=tb_name)
        print(response_get_table)
        return True
    except ClientError as error:
        if error.response['Error']['Code'] == "EntityNotFoundException":
            LOGGER.info(f"""{tb_name} table doesn't exist in %s {region_name}. Creating {tb_name}.""")
            return False
        subject = f"""{region_name}-Asset Allocation Load: Classification Lambda failed."""
        sns_client.publish(
            TopicArn=os.environ['sns_arn'],
            Message=f"""Classification lambda failed due to : {error}""",
            subject=subject
        )
        LOGGER.error(f"""Error in reading Glue table due to : {error}""")
        raise error


@log_methods_non_sensitive
def database_exists(db_name: str, region_name: str) -> bool:
    """Check if database exists in the given region.
    :param db_name: String -> of Database name in AWS Glue
    :param region_name: String -> of the region in the AWS Glue database
    :return: bool
    """

    try:
        client = boto3.client('glue', region_name=region_name)
        response = client.get_databases()
        database_list = response['DatabaseList']
        for database in database_list:
            if db_name == database['Name']:
                LOGGER.info("Database exists : %s", db_name)
                return True
        return False
    except ClientError as error:
        subject = f"""{region_name}-Asset Allocation Load: Classification Lambda failed."""
        sns_client.publish(
            TopicArn=os.environ['sns_arn'],
            Message=f"""Classification lambda failed due to : {error}""",
            subject=subject
        )
        LOGGER.error("Error in getting Glue database: %s", error)
        raise error


@log_methods_non_sensitive
def create_database(db_name: str, region_name: str) -> None:
    """Create database in the given region.
    :param db_name: String -> of Database name in AWS Glue
    :param region_name: String -> of the region in the AWS Glue database
    """ 
    try:
        client = boto3.client('glue', region_name=region_name)
        client.create_database(
            DatabaseInput={'Name': db_name})
        LOGGER.info("Database created: %s", db_name)
    except ClientError as error:
        subject = f"""{region_name}-Asset Allocation Load: Classification Lambda failed."""
        sns_client.publish(
            TopicArn=os.environ['sns_arn'],
            Message=f"""Classification lambda failed due to : {error}""",
            subject=subject
        )
        LOGGER.error("Error in Creating Glue database: %s", error)
        raise error


@log_methods_non_sensitive
def create_table(db_name: str, 
                 table_name: dict, 
                 region_name: str,
                 column: list,
                 location: str,
                 bucket: str) -> None:
    """Create table in the given database and region.
    :param db_name: String -> of Database name in AWS Glue
    :param table_name: dict -> of table in AWS Glue
    :param region_name: String -> of the region in the AWS Glue database
    """
    try:
        client = boto3.client('glue', region_name=region_name)
        client.create_table(
            DatabaseName=db_name,
            TableInput={
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': column,
                    'Location': f"s3://{bucket}/{location}",
                    'InputFormat': (
                        'org.apache.hadoop.hive.ql.io.parquet'
                        '.MapredParquetInputFormat'
                    ),
                    'OutputFormat': 
                        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'NumberOfBuckets': -1,
                    'SerdeInfo': {
                        'SerializationLibrary':
                            'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                        'Parameters': {
                            'serialization.format': '1'
                        }
                    }
                },
                'PartitionKeys': [
                    {
                        "Name": "yyyy",
                        "Type": "int"
                    },
                    {
                        "Name": "mm",
                        "Type": "int"
                    },
                    {
                        "Name": "dd",
                        "Type": "int"
                    },
                    {
                        "Name": "version_number",
                        "Type": "int"
                    }
                ],
                'TableType': 'EXTERNAL_TABLE',
                'Parameters': {
                    'classifications': 'parquet'
                }
            }
        )
        LOGGER.info("Table created : %s", table_name)
    except ClientError as err:
        if err.response['Error']['Code'] == "AlreadyExistsException":
            logging.info("%s table already exists.", table_name)
            subject = f"""{region_name}-Asset Allocation Load: Classification Lambda failed."""
            sns_client.publish(
                TopicArn=os.environ['sns_arn'],
                Message=f"""Classification lambda failed due to : {err}""",
                subject=subject
            )
        else:
            raise err


def create_and_update_partitions(
        bucket: str, database: str, table: str, year: str, month: str, day: str,
        version_number: str, data_file_s3_location: str, region_name: str) -> None:
    """Create partition if does not exist for input date, update partition if it does."""

    final_partition_location = f"""s3://{bucket}/{data_file_s3_location}/yyyy={year}/
                               mm={month}/dd={day}/version_number={version_number}"""
    glue = boto3.client('glue', region_name=region_name)

    # Need to check if partition already exists.
    response = glue.get_partitions(
        DatabaseName=database,
        TableName=table
    )

    table_partition_values = []

    for partition in response['Partitions']:
        table_partition_values.append(partition['Values'])

    # Get table details used in creating Partitions
    table_response = glue.get_table(
        DatabaseName=database,
        Name=table
    )

    storage_descriptor = table_response['Table']['StorageDescriptor']
    custom_storage_descriptor = copy.deepcopy(storage_descriptor)
    custom_storage_descriptor['Location'] = final_partition_location
    partition_values = [year, month, day, version_number]

    if [year, month, day, version_number] not in table_partition_values:
        glue.create_partition(
            DatabaseName=database,
            TableName=table,
            PartitionInput={
                'Values': partition_values,
                'StorageDescriptor': custom_storage_descriptor
            }
        )
    else:
        glue.update_partition(
            DatabaseName=database,
            TableName=table,
            PartitionInput={
                'Values': partition_values,
                'StorageDescriptor': custom_storage_descriptor
            }
        )      