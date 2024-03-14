# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3, json, sys, os
from botocore.config import Config
from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')

# connect to AWS resources (S3, dynamo, sqs) and get AWS credentials
aws_config = Config(region_name=config['aws']['AwsRegionName'], signature_version=config['aws']['SignatureVersion'])
try: 
    s3_glacier = boto3.client('glacier')
    s3_resource = boto3.resource('s3', config=aws_config)
    dynamo = boto3.resource('dynamodb')
    dynamo_table = dynamo.Table(config['aws']['DynamoTableName'])
    sqs = boto3.client('sqs')
    glacier_sqs_url = config["aws"]["GlacierSqs"]
except ClientError as e:
    print(f"Unexpected error in AWS config in archive.py: {str(e)}")
    exit()


# https://stackoverflow.com/questions/41833565/s3-buckets-to-glacier-on-demand-is-it-possible-from-boto3-api
if __name__ == '__main__':
    # poll SQS message queue for free user archiving
    while True:
        print("Polling for files to archive...")

        # Long poll for message on Glacier SQS queue
        response = sqs.receive_message(
            QueueUrl=glacier_sqs_url,
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
                job_id = message_body["job_id"]
                s3_key_results_file = message_body["results_file_location"]
                user_id = message_body["user_id"]
            except KeyError as e:
                # keep message in queue, go to next iteration of loop
                print(f"Error decoding message to archive S3 file for job: {str(e)}")
                continue

            # dynamically check if this is a free user
            user_profile = helpers.get_user_profile(id=user_id)
            if user_profile["role"] == "free_user":
                ## https://stackoverflow.com/questions/41833565/s3-buckets-to-glacier-on-demand-is-it-possible-from-boto3-api
                bucket = s3_resource.Bucket(config["aws"]["ResultsBucketName"])

                # filter for specific S3 results file using a filter
                for obj in bucket.objects.filter(Prefix=s3_key_results_file):
                    print("Free user, archiving files to Glacier...")
                    try:
                        glacier_response = s3_glacier.upload_archive(vaultName=config["aws"]["GlacierVaultName"], body=obj.get()['Body'].read())
                        archive_id = glacier_response["archiveId"]
                    except ClientError as e:
                        print(f"Error moving S3 file for job {job_id} to Glacier: {str(e)}") 
                        continue

                    # update DynamoDB item for this job to store the location of it in Glacier vault
                    try:
                        response = dynamo_table.update_item(
                            Key={'job_id': job_id},
                            UpdateExpression="SET #attr1 = :archive_id",
                            ExpressionAttributeNames={'#attr1': 'results_file_archive_id'},
                            ExpressionAttributeValues={':archive_id': archive_id}
                        )
                    except Exception as msg:
                        print(f"Oops, could not update DynamoDB for archiving job results for job {job_id}: {str(msg)}")                    
                    
                    # delete file from S3
                    obj.delete()
                    print(f"Archive successful. Deleted results file for job {job_id} from S3.")
            else:
                print("Not a free user, won't be archiving their file...")

            # delete message from the queue since already processed, whether or not archiving happened
            try: 
                response = sqs.delete_message(
                    QueueUrl=glacier_sqs_url,
                    ReceiptHandle=request_body['ReceiptHandle']
                )
            except ClientError as e:
                print(f"Error deleting SQS message for archiving job {job_id}: {str(e)}")                    


### EOF