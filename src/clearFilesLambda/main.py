"""Runs as Clear Files on alert Lambda.

Raises-
    error: lambda exception
Returns-
    [dict]:

"""
import os
import logging
import boto3

logging.getLogger().setLevel(logging.INFO)

bucket_name = os.environ['bucket_name']
source_key = os.environ['data_processing_folder']
dest_key = os.environ['error_folder']


def error_files_mover():
    s3_client = boto3.client("s3")
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    final_source_key = "asset_allocation_data" + source_key

    for bucket_object in bucket.objects.filter(Prefix=final_source_key):
        path, file = os.path.split(bucket_object.key)
        copy_source_object = {
            'Bucket': bucket_name,
            'Key': f"""{path}/{file}"""
        }
        if file == "":
            continue
        s3_client.copy_object(
            CopySource=copy_source_object,
            Bucket=bucket_name,
            Key=f"""{dest_key}/{file}"""
        )
        s3_client.delete_object(
            Bucket=bucket_name,
            Key=f"""{path}/{file}"""
        )
    return "Success"


def lambda_handler(event: dict, _context: dict) -> dict:
    """Main lambda handler for Clear Files on alert Lambda."""

    # In case a correct event is encountered -------------------------------------------
    if event:
        try:
            logging.info("This is the event we received: %s", event)
            error_files_mover()
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