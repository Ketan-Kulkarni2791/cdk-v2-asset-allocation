"""Runs as Placeholder 2 Lambda.

Raises-
    error: lambda exception
Returns-
    [dict]:

"""
import os
import logging
import io
import boto3
import pandas as pd
from datetime import datetime

logging.getLogger().setLevel(logging.INFO)

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')


def get_csv_content_from_s3(s3_bucket, key):
    file_obj = s3_client.get_object(Bucket=s3_bucket, Key=key)
    file_content = file_obj["Body"].read()
    read_csv_data = io.BytesIO(file_content)
    input_df = pd.read_csv(read_csv_data)
    return input_df


def lambda_handler(event: dict, _context: dict) -> dict:
    """Main lambda handler for Placeholder 2 Lambda."""

    # In case a correct event is encountered -------------------------------------------
    if event:
        try:
            logging.info("This is the event we received: %s", event)
            file_name = event['file_name']
            file_version = event['output']['Payload']['file_version']
            file_date = file_name.split('.')[0].split('_')[1]
            file_date = str(datetime.strptime(file_date, "%m%d%y")).split(' ')[0]
            folder_name = event['folder_name']
            env = os.environ['env']
            region = os.environ['region']
            year, month, day = file_date.split('-')[0], file_date.split('-')[1], file_date.split('-')[2]
            etag = event['etag']

            bucket = os.environ['bucket_name']

            key = f"""{folder_name}/{file_name}"""
            print(key)

            input_df = get_csv_content_from_s3(bucket, key)
            input_df.to_parquet(
                f"""s3://{bucket}/asset_allocation_data/outbound/asset_alloc_table/yyyy={year}/
                mm={month}/dd={day}/version_number={file_version}/
                TAA_{year}_{month}_{day}.parquet""", index=False
            )

            event = {
                "file_name": file_name,
                "folder_name": folder_name,
                "file_version": file_version,
                "etag": etag,
                "stage": "metadata update"
            }
            
            return event
        # In case an error is encountered even when a correct event is present ---------
        except Exception as error:
            logging.error("An error occurred: %s", error)
            raise error
    # In case no event is present ------------------------------------------------------
    else:
        logging.error("We couldn't find a suitable event. Exiting....")
        raise OSError("No event found")