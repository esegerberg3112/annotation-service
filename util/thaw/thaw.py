# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3, json, subprocess, sys, os
from botocore.config import Config
from botocore.exceptions import ClientError, ParamValidationError
from boto3.dynamodb.conditions import Key

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')

# connect to AWS resources (S3, dynamo, sqs) and get AWS credentials
aws_config = Config(region_name=config['aws']['AwsRegionName'], signature_version=config['aws']['SignatureVersion'])
try: 
    s3_glacier = boto3.client('glacier')
    sqs = boto3.client('sqs')
    s3 = boto3.client('s3', config=aws_config)
    dynamo = boto3.resource('dynamodb')
    dynamo_table = dynamo.Table(config['aws']['DynamoTableName'])
except ClientError as e:
    print(f"Unexpected error in AWS config inside thaw.py: {str(e)}")
    exit()

if __name__ == '__main__':
    # pull down request to restore files from Glacier
    while True:
        print("Polling to check on progress of restored Glacier archives...")
       # Long poll for message on restoring Glacier archives SQS queue
        response = sqs.receive_message(
            QueueUrl=config["aws"]["SQSRetrieveGlacierFiles"],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=2
        )

        # a message was found
        if 'Messages' in response:
            request_body = response["Messages"][0]
            payload = json.loads(request_body["Body"])
            message_body = json.loads(payload.get("Message"))

            # extract values from received message
            try:
                glacier_retrieval_job_id = message_body["glacier_retrieval_job_id"]
                archive_id = message_body["archive_id"]
                s3_results_key_name = message_body["s3_results_key_name"]
                annotation_job_id = message_body["annotation_job_id"]
            except KeyError as e:
                print(f"Error decoding message when polling thaw requests: {str(e)}")
                continue

            # check if job is finished. If not, don't delete message. Will recycle into queue after 30 seconds due to Visbility Timeout
            job_status = s3_glacier.describe_job(
                vaultName=config['aws']['GlacierVaultName'],
                jobId=glacier_retrieval_job_id
            )['StatusCode']

            print(f"Status of thaw job: {job_status}")

            ### https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/delete_archive.html
            # if unarchiving job is done, get its output and upload to S3
            if job_status == "Succeeded":
                job_output = s3_glacier.get_job_output(
                    vaultName=config['aws']['GlacierVaultName'],
                    jobId=glacier_retrieval_job_id
                )
                data = job_output['body'].read() 

                # put file into S3
                s3.put_object(Body=data, Bucket=config['aws']['ResultsBucketName'], Key=s3_results_key_name)
                print(f"Successfully thawed Glacier file {s3_results_key_name} and moved to S3...")

                # delete archive inside Glacier
                s3_glacier.delete_archive(
                    vaultName=config['aws']['GlacierVaultName'],
                    archiveId=archive_id
                )
                print("Deleted Glacier archive now that copy moved to S3...")

                # delete message from queue since this annotation result has been successfully unarchived
                try: 
                    response = sqs.delete_message(
                        QueueUrl=config["aws"]["SQSRetrieveGlacierFiles"],
                        ReceiptHandle=request_body['ReceiptHandle']
                    )
                except ClientError as e:
                    print(f"Error deleting SQS message for restoring results file for glacie job {glacier_retrieval_job_id}: {str(e)}")

                # update Dynamo entry for this annotation job to overwrite it's archive file, as it should never be un-archived again
                try:
                    response = dynamo_table.update_item(
                        Key={'job_id': annotation_job_id},
                        UpdateExpression="SET #attr1 = :archive_id",
                        ExpressionAttributeNames={'#attr1': 'results_file_archive_id'},
                        ExpressionAttributeValues={':archive_id': ""}
                    )
                except Exception as msg:
                    print(f"Oops, could not update DynamoDB for successfully unarchive of Glacier file {s3_results_key_name}: {str(msg)}")                
                
            elif job_status == "Failed":
                print(f"Error occurred when trying to get Glacier job with ID: {glacier_retrieval_job_id}") 
                continue
            else:
                continue

### EOF