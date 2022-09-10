"""Runs as Validation Trigger Lambda.

Raises-
    error: lambda exception
Returns-
    [dict]:

"""
import os
# import io
# import json
import logging
# import uuid
# import pandas as pd
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

# def get_csv_content_from_s3(bucket_name, key):
#     file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
#     file_content = file_obj["Body"].read()
#     read_csv_data = io.BytesIO(file_content)
#     df = pd.read_csv(read_csv_data)
#     return df


def lambda_handler(event: dict, _context: dict) -> dict:
    """Main lambda handler for Incoming Data to S3 Transform Location Lambda."""

    # In case a correct event is encountered -------------------------------------------
    if event:
        try:
            logging.info("This is the event we received: %s", event)
            # identifier = str(uuid.uuid1())
            folder_name = event['Records'][0]['s3']['object']['key']
            print(f"------------ folder_name : {folder_name}")
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