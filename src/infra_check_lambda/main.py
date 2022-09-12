"""Module for validating and creating data tables and metadata table.

Raises-
    error: lambda exception
Returns-
    [dict]:

"""
import os
import logging
import boto3
from datetime import datetime

from code_lib.decorators import log_methods_non_sensitive
from code_lib.glue_utils import database_exists, create_database, create_table, table_exists, create_and_update_partitions
from code_lib.table_schema import tableSchemas

logging.getLogger().setLevel(logging.INFO)

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


@log_methods_non_sensitive
def lambda_handler(event: dict, _context: dict) -> dict:
    """Main lambda handler for Placeholder 1 Lambda."""

    # In case a correct event is encountered -------------------------------------------
    if event:
        try:
            logging.info("This is the event we received: %s", event)
            region = os.environ['region']
            env = os.environ['env']
            database = os.environ['database']
            file_name = event['file_name']
            file_date = file_name.split('.')[0].split('_')[1]
            year, month, day = file_date.split(
                '-')[0], file_date.split('-')[1], file_date.split('-')[2]
            folder_name = event['folder_name']
            etag = event['etag']
            insertion_date = str(datetime.now()).split(' ')[0]

            asset_alloc_table = os.environ['asset_alloc_table']
            asset_alloc_table_location = os.environ['asset_alloc_table_location']
            bucket = os.environ['bucket_name']

            if 'output' in event:
                stage = event['output']['Payload']['stage']
            else:
                stage = event['stage']

            print(stage)

            if stage == 'infra check':

                if not database_exists(database, region):
                    create_database(database, region)
                    create_table(
                        database, asset_alloc_table, region, tableSchemas.asset_alloc_data,
                        asset_alloc_table_location, bucket
                    )
                else:
                    if not table_exists(database, asset_alloc_table, region):
                        create_table(
                            database, asset_alloc_table, region, tableSchemas.asset_alloc_data,
                            asset_alloc_table_location, bucket
                        )

            return {
                'Success': "Event found"
            }
        # In case an error is encountered even when a correct event is present ---------
        except Exception as error:
            logging.error("An error occurred: %s", error)
            raise error
    # In case no event is present ------------------------------------------------------
    else:
        logging.error("We couldn't find a suitable event. Exiting....")
        raise OSError("No event found")