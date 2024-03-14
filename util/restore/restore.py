# restore.py
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
config.read('restore_config.ini')

# connect to AWS resources (S3, dynamo, sqs) and get AWS credentials
aws_config = Config(region_name=config['aws']['AwsRegionName'], signature_version=config['aws']['SignatureVersion'])
try: 
    s3_glacier = boto3.client('glacier')
    dynamo = boto3.resource('dynamodb')
    dynamo_table = dynamo.Table(config['aws']['DynamoTableName'])
    sqs = boto3.client('sqs')
    sns = boto3.client('sns')
except ClientError as e:
    print(f"Unexpected error in AWS config inside restore.py: {str(e)}")
    exit()

if __name__ == '__main__':
    # poll SQS message queue for request from new premium user to restore their archived files
    while True:
        print("Polling for requests to restore archived files...")

        # Long poll for message on restoring Glacier archives SQS queue
        response = sqs.receive_message(
            QueueUrl=config["aws"]["SQSRestoreGlacier"],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5
        )

        # a message was found
        if 'Messages' in response:
            request_body = response["Messages"][0]
            payload = json.loads(request_body["Body"])
            message_body = json.loads(payload.get("Message"))

            # extract values from received message
            try:
                user_id = message_body["user_id"]
            except KeyError as e:
                print(f"Error decoding message to kick off glacier retrieval progress job: {str(e)}")
                continue

            # get all archived files that this user has, and initiate job to retrieve archived file from Glacier for each
            user_files = dynamo_table.query(
                IndexName=config["aws"]["DynamoSecondaryIndex"],
                KeyConditionExpression=Key(config["aws"]["DynamoSecondaryPartitionKey"]).eq(user_id)
            )

            # loop through all annotations this user has in Dynamo
            for annotation in user_files["Items"]:
                annotation_job_id = annotation["job_id"]
                # only un-archive files that were actually placed in Glacier
                if "results_file_archive_id" in annotation and annotation["results_file_archive_id"] != "":
                    # get the archive ID for each file and initiate thawing job for each
                    archive_id = annotation["results_file_archive_id"]
                    s3_results_key_name = annotation["s3_key_result_file"]

                    ### https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
                    # start with an Expedited retrieval, and default to Standard
                    initiate_status = False
                    try:
                        job_response = s3_glacier.initiate_job(
                            vaultName=config["aws"]["GlacierVaultName"],
                            jobParameters={
                                'Type': 'archive-retrieval', 
                                'ArchiveId': archive_id,
                                'Tier': "Expedited"
                            }
                        )
                        initiate_status = True
                    except s3_glacier.exceptions.InsufficientCapacityException as e:
                        print("Expedited retrieval failed, trying standard...")
                        try: 
                            job_response = s3_glacier.initiate_job(
                                vaultName=config["aws"]["GlacierVaultName"],
                                jobParameters={
                                    'Type': 'archive-retrieval', 
                                    'ArchiveId': archive_id
                                }
                            )
                            initiate_status = True
                        except ClientError as e:
                            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                                print(f"Archive {archive_id} was already restored from Glacier and deleted in Glacier.")
                            else:
                                print(e)
                    except ClientError as e:
                            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                                print(f"Archive {archive_id} was already restored from Glacier and deleted in Glacier.")
                            else:
                                print(e)

                    if initiate_status:
                        print(f"Initiated glacier retrieval job: {job_response} for job {annotation_job_id}")
                        data_obj = {
                            "glacier_retrieval_job_id": job_response['jobId'],
                            "archive_id": archive_id,
                            "s3_results_key_name": s3_results_key_name,
                            "annotation_job_id": annotation_job_id
                        }

                        # post this job ID to SNS for glacier retrievals to check on its progress
                        try: 
                            response = sns.publish(
                                TopicArn=config["aws"]["SNSGlacialRetrievalARN"],
                                Message=json.dumps(data_obj),
                                MessageStructure='string',
                            )
                            print(f"Successfully posted Glacier retrieval job to SNS to poll for thawing...")
                        except ParamValidationError as e:
                            print(f"Error publishing to SNS to begin Glacier retrieval for {job_response['jobId']}: {str(e)}")               
                        except KeyError as e:
                            print(f"Error publishing to SNS to begin Glacier retrieval for {job_response['jobId']}: {str(e)}")   
                else:
                    print(f"File not archived, do not need to initiatie Glacier retrieval for job {annotation_job_id}")

            # delete message from the queue since already processed
            try: 
                response = sqs.delete_message(
                    QueueUrl=config["aws"]["SQSRestoreGlacier"],
                    ReceiptHandle=request_body['ReceiptHandle']
                )
                print(f"Successfully deleted Glacier restore job from queue...")
            except ClientError as e:
                print(f"Error deleting SQS message for restoring results file for user {user_id}: {str(e)}")                
            

### EOF