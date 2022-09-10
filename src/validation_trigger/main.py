"""Runs as Validation Trigger Lambda.

Raises-
    error: lambda exception
Returns-
    [dict]:

"""
import os
import io
import json
import logging
import uuid
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
    input_df = pd.read_csv(read_csv_data)
    return input_df


def error_files_mover():
    bucket = s3.Bucket(bucket_name)
    final_source_key = source_key

    for bucket_object in bucket.objects.filter(Prefix=final_source_key):
        path, file = os.path.split(bucket_object.key)
        copy_source_object = {
            'Bucket': bucket_name,
            'Key': f"""{path/file}"""
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
            Key=f"""{path/file}"""
        )
    return "Success"


def lambda_handler(event: dict, _context: dict) -> dict:
    """Main lambda handler for Incoming Data to S3 Transform Location Lambda."""

    # In case a correct event is encountered -------------------------------------------
    if event:
        try:
            logging.info("This is the event we received: %s", event)
            identifier = str(uuid.uuid1())
            folder_name = event['Records'][0]['s3']['object']['key']
            folder_name = folder_name.split('/')[-2]
            file_key = event['Records'][0]['s3']['object']['key']
            file_name = file_key.split('/')[-1]
            etag = event['Records'][0]['s3']['object']['eTag']
            s3_bucket = event['Records'][0]['s3']['bucket']['name']

            input_df = get_csv_content_from_s3(s3_bucket, file_key)
            column_names_actual = ['pfg_ast_clss_nm', 'pfg_sblvl_1_nm', 'pfg_sblvl_1_5_nm',
                                   'pfg_sblvl_2_nm', 'pfg_sblvl_3_nm', 'pfg_sblvl_4_nm',
                                   'wal_nm', 'LEVEL', 'amount_type', 'amount']
            column_names_fetched = []
            for i in input_df.columns:
                column_names_fetched.append(i)
            column_names_fetched = list(
                filter(lambda x: not x.startswith("Unnamed:"), column_names_fetched)
            )

            if not input_df.empty and (all(x in column_names_fetched for x in column_names_actual)):
                amt_type_col_actuals = ['invest_pct', 'taa_pct', 'saa_pct']
                amt_type_col_fetched = pd.unique(input_df['amount_type']).tolist()

                if all(i in amt_type_col_fetched for i in amt_type_col_actuals):
                    data_actuals = [10.0, 10.0, 10.0]
                    data_fetched = input_df.groupby("amount_type")["amount"].sum()
                    data_fetched = [float("{0:.2f}".format(i)) for i in data_fetched]

                    if all(i in data_fetched for i in data_actuals):
                        input_message_to_sfn = {
                            'file_name': file_name,
                            'folder_name': f"asset_allocation_data/{folder_name}",
                            'etag': etag,
                            'stage': 'infra check'
                        }
                        stepfunction_client.start_execution(
                            stateMachineArn=os.environ['stateMachineArn'],
                            name=identifier,
                            input=json.dumps(input_message_to_sfn)
                        )
                        print("Started Step Function..")
                        return {"Success": "Event found"}
                    else:
                        print("Some of the data purity check is failing in 'amount' column. Exiting...")
                        error_files_mover()
                        sns_client.publish(
                            TopicArn=os.environ['sns_arn'],
                            Message="Data Purity Failure in 'amount' column.",
                            Subject="Data Purity Failure in 'amount' column"
                        )
                        return {"Status": "Failed"}

                else:
                    data_diff = set(amt_type_col_actuals).difference(set(amt_type_col_fetched))
                    print("Some data availability check is failing in 'amount_type' column. Existing..")
                    error_files_mover()
                    sns_client.publish(
                        TopicArn=os.environ['sns_arn'],
                        Message=f"Following data type is not present in 'amount_type' column : {data_diff}",
                        Subject="Some data availability failure in 'amount_type' column"
                    )
                    return {"Status": "Failed"}

            else:
                data_diff = set(column_names_actual).difference(set(column_names_fetched))
                print("Either the file is empty or some of the columns are mismatched. Existing..")
                error_files_mover()
                sns_client.publish(
                    TopicArn=os.environ['sns_arn'],
                    Message=f"Following columns are mismatched : {data_diff}",
                    Subject="Either the file is empty or some of the columns are mismatched"
                )
                return {"Status": "Failed"}

        # In case an error is encountered even when a correct event is present ---------
        except Exception as error:
            error_files_mover()
            logging.error("An error occurred: %s", error)
            sns_client.publish(
                TopicArn=os.environ['sns_arn'],
                Message="Error occurred.",
                Subject="Failure in step function trigger lambda"
            )
            raise error
    # In case no event is present ------------------------------------------------------
    else:
        logging.error("We couldn't find a suitable event. Exiting....")
        raise OSError("No event found")