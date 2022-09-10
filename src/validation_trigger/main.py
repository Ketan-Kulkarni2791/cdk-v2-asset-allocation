"""Runs as Validation Trigger Lambda.

Raises-
    error: lambda exception
Returns-
    [dict]:

"""
import os
import io
# import json
import logging
# import uuid
import pandas as pd
import boto3

logging.getLogger().setLevel(logging.INFO)

stepfunction_client = boto3.client("stepfunctions")
s3_client = boto3.client("s3")
s3 = boto3.resource("s3")
sns_client = boto3.client("sns")

environment = os.environ['env']
bucket_name = os.environ['bucket_name']
source_key = os.environ['trigger_prefix']
dest_key = os.environ['error_folder']


def get_csv_content_from_s3(s3_bucket, key):
    file_obj = s3_client.get_object(Bucket=s3_bucket, Key=key)
    file_content = file_obj["Body"].read()
    read_csv_data = io.BytesIO(file_content)
    df = pd.read_csv(read_csv_data)
    return df


def lambda_handler(event: dict, _context: dict) -> dict:
    """Main lambda handler for Incoming Data to S3 Transform Location Lambda."""

    # In case a correct event is encountered -------------------------------------------
    if event:
        try:
            logging.info("This is the event we received: %s", event)
            # identifier = str(uuid.uuid1())
            folder_name = event['Records'][0]['s3']['object']['key']
            folder_name = folder_name.split('/')[-2]
            file_key = event['Records'][0]['s3']['object']['key']
            # file_name = file_key.split('/')[-1]
            # etag = event['Records'][0]['s3']['object']['eTag']
            s3_bucket = event['Records'][0]['s3']['bucket']['name']

            input_df = get_csv_content_from_s3(s3_bucket, file_key)
            print(input_df)            
            
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