import boto3, json, subprocess, os
from botocore.config import Config
from botocore.exceptions import ClientError
from configparser import SafeConfigParser

# Get annotator configuration
config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

# connect to AWS resources (S3, dynamo, sns) and get AWS credentials
aws_config = Config(region_name=config['aws']['AwsRegionName'], signature_version=config['aws']['SignatureVersion'])
try: 
    s3_client = boto3.client('s3', config=aws_config)
    dynamo = boto3.resource('dynamodb')
    dynamo_table = dynamo.Table(config['aws']['DynamoTableName'])
    sqs = boto3.client('sqs')
    sqs_url = config['aws']['SqsUrl']
except ClientError as e:
    print(f"Unexpected error in AWS config in annotator.py: {e}")
    exit()

def make_new_directory(file_path: str): 
    """ 
    Helper function to create new file directories as needed 
    """
    if not os.path.exists(file_path):
        os.makedirs(file_path)
        print(f"Directory '{file_path}' created.")
    else:
        print(f"Directory '{file_path}' already exists.")

# when this script is ran, create a folder to hold annotation jobs submitted if it doesn't exist already
new_dir = config['annotation_output']['OutputFolder']
make_new_directory(new_dir)

if __name__ == '__main__':
    # poll SQS message queue
    while True:
        print("Polling for new annotation job requests...")

        # Long poll for message on provided SQS queue
        response = sqs.receive_message(
            QueueUrl=sqs_url,
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
                input_file_name = message_body["input_file_name"]
                s3_bucket = message_body["s3_inputs_bucket"]
                s3_key = message_body["s3_key_input_file"]
                user_id = message_body["user_id"]
                user_email = message_body["user_email"]
                message_id = payload["MessageId"]
            except KeyError as e:
                print(f"Error decoding message to kick off annotation job: {e}")
                continue 

            # make a new sub-directory for this job so can persist unique outfiles
            new_job_directory = config['annotation_output']['OutputFolder'] + '/' + job_id
            make_new_directory(new_job_directory)
            downloaded_file_path = new_job_directory + "/" + input_file_name

            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
            # download S3 file into current EC2 instance, write it to a file in a folder unique to its job id
            try: 
                with open(downloaded_file_path, 'wb') as f:
                    s3_client.download_fileobj(s3_bucket, s3_key, f)
            except ClientError as e:
                print(f"Error downloading S3 file for {job_id} to run annotation on: {e}")
                continue

            # execute run.py subprocess for this job
            submit_command = ['python', 'run.py', downloaded_file_path, "--parameter1", job_id, "--parameter2", input_file_name, "--parameter3", user_id, "--parameter4", user_email]
            try:
                # check the run.py file exists
                with open("run.py", 'r') as file:
                    # Process the file as needed
                    file_contents = file.read()
                process = subprocess.Popen(submit_command)
                print(f"Annotation job started for {job_id}")
            except FileNotFoundError:
                print(f"Error when running annotation for {job_id}: {e}")
                continue
            # Check if the subprocess is still running or if it encountered an error finding the file
            except Exception as e:
                print(f"Error when running annotation for {job_id}: {e}")
                continue

            # submitting annotation job was successful, so delete message from the queue since already processed
            try: 
                response = sqs.delete_message(
                    QueueUrl=sqs_url,
                    ReceiptHandle=request_body['ReceiptHandle']
                )
            except ClientError as e:
                print(f"Error deleting SQS message upon successful annotation: {e}")
                continue
        
            # https://stackoverflow.com/questions/34447304/example-of-update-item-in-dynamodb-boto3
            # update job in DynamoDB to RUNNING only if currently PENDING
            try:
                response = dynamo_table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression="SET #attr1 = :new_status",
                    ConditionExpression="job_status = :expected_status",
                    ExpressionAttributeNames={'#attr1': 'job_status'},
                    ExpressionAttributeValues={':new_status': 'RUNNING', ':expected_status': 'PENDING'}
                )
            except Exception as msg:
                print(f"Oops, could not update DynamoDB for annotation job results on job {job_id}: {str(msg)}")