###
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3, driver, json, os, shutil, sys, time
from botocore.config import Config
from botocore.exceptions import ClientError, ParamValidationError
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
    sns = boto3.client('sns')
except ClientError as e:
    print(f"Unexpected error: {e}")
    exit()

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')
            bucket_name = config['aws']['ResultsBucketName']

            try:
                job_id = sys.argv[3]
                input_file_name = sys.argv[5]
                user_id = sys.argv[7]
                user_email = sys.argv[9]
            except IndexError as e:
               print("Job ID parameter not given")
               exit()
            
            file_prefix = input_file_name[:-4]
            annot_file = file_prefix + ".annot.vcf"
            log_file = file_prefix + ".vcf.count.log"
            s3_key_name = config['aws']['BucketObjectRoot'] + "/" + user_id + "/"

            # define local job directory to clean up once files are uploaded to S3
            clean_up_folder = config['annotation_output']['OutputFolder'] + "/" + job_id

            # upload annotation job output files to S3
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
            try:
                with open(clean_up_folder + "/" + annot_file, "rb") as file1:
                    s3_client.upload_fileobj(file1, bucket_name, s3_key_name + job_id + "~" + annot_file)
                with open(clean_up_folder + "/" + log_file, "rb") as file2:
                    s3_client.upload_fileobj(file2, bucket_name, s3_key_name + job_id + "~" + log_file)
            except ClientError as e:
               print(e)

            # update DynamoDB with output files
            try:
                dynamo_table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression="SET #attr1 = :new_status, #attr2 = :result_file, #attr3 = :log_file, #attr4 = :complete_time, #attr5 = :bucket",
                    ConditionExpression="job_status = :expected_status",
                    ExpressionAttributeNames={
                       '#attr1': 'job_status',
                       '#attr2': 's3_key_result_file',
                       '#attr3': 's3_key_log_file',
                       '#attr4': 'complete_time',
                       '#attr5': 's3_results_bucket'
                    },
                    ExpressionAttributeValues={
                       ':new_status': 'COMPLETED', 
                       ':expected_status': 'RUNNING',
                       ':result_file': s3_key_name + job_id + "~" + annot_file,
                       ':log_file': s3_key_name + job_id + "~" + log_file,
                       ':complete_time': int(time.time()),
                       ':bucket': bucket_name
                    }
                )
            except Exception as msg:
                print(f"Oops, could not update DynamoDB: {str(msg)}")

            # annotation job is complete, notify SNS results topic
            data_obj = {
                "job_id": job_id,
                "user_id": user_id,
                "input_file_name": input_file_name,
                "complete_time": int(time.time()),
                "job_status": "COMPLETED",
                "results_file_location": s3_key_name + job_id + "~" + annot_file,
                "user_email": user_email
            }
            try: 
                response = sns.publish(
                    TopicArn=config['aws']['SNSResultsTopicARN'],
                    Message=json.dumps(data_obj),
                    MessageStructure='string',
                )
            except ParamValidationError as e:
                print(f"Error: {e}")

            ## https://www.scaler.com/topics/delete-directory-python/
            # clean up local folder
            try:
              shutil.rmtree(clean_up_folder)
              print("Directory removed successfully")
            except OSError as o:
                print(f"Error, {o.strerror}: {clean_up_folder}")
    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF