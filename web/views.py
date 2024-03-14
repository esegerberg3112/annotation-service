# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError, ParamValidationError

from flask import (abort, flash, make_response, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile

# Create AWS connections on startup
  # Create a session client to the S3 service
s3 = boto3.client('s3',
  region_name=app.config['AWS_REGION_NAME'],
  config=Config(signature_version='s3v4'))
sns = boto3.client('sns')
dynamo = boto3.resource('dynamodb')
dynamo_table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])


#<--------------------------------------------- APP ROUTES ------------------------------------>

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  aws_input_bucket = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'
  
  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  define_fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  define_conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=aws_input_bucket, 
      Key=key_name,
      Fields=define_fields,
      Conditions=define_conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post, file_id=key_name)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():
    # get user profile
    profile = get_profile(identity_id=session.get('primary_identity'))

    try:
        # extract bucket, key from the form data in S3 redirect url
        s3_bucket = request.args.get('bucket')
        s3_key = request.args.get('key')
    except Exception as e:
        app.logger.error(f"Unable to get information from S3 redirect: {e}")
        abort(405)

    # create unique job ID for this annotation job
    job_id = str(uuid.uuid4())

    # get just the filename from the s3_key, formatted like <uuid>~<filename>.vcf
    start_index = s3_key.find("~")
    input_file_name = s3_key[start_index+1:]

    # create data object to persist to annotations database in DynamoDB
    data_obj = {
        "job_id": job_id,
        "user_id": session['primary_identity'],
        "input_file_name": input_file_name,
        "s3_inputs_bucket": s3_bucket,
        "s3_key_input_file": s3_key,
        "submit_time": int(time.time()),
        "job_status": "PENDING",
        "user_email": session["email"],
    }
    try:
        dynamo_table.put_item(Item = data_obj)
        app.logger.info(f"Added new annotation job data to DynamoDB...")
    except ClientError as e:
        app.logger.error(f"Error adding new annotation job to Dynamo: {e}")
        abort(500)
    except EndpointConnectionError as e:
        app.logger.error(f"Error adding new annotation job to Dynamo: {e}")
        abort(500)

    # post new job request to SNS topic for new annotation requests
    try: 
        response = sns.publish(
            TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'], 
            Message=json.dumps(data_obj),
            MessageStructure='string',
        )
        app.logger.info(f"Successfully published newly submitted job {job_id} to SNS topic.")
    except ParamValidationError as e:
        app.logger.error(f"Could not post new job request to SNS: {e}")
        abort(500)

    return render_template('annotate_confirm.html', job_id=job_id, file_name=input_file_name)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  current_user_id = session['primary_identity']
  # query database using secondary index and matching on user_id partition key 
  response = dynamo_table.query(
      IndexName=app.config['AWS_DYNAMODB_SECONDARY_INDEX'],
      KeyConditionExpression=Key(app.config['AWS_DYNAMODB_SECONDARY_PARTITION_KEY']).eq(current_user_id)
  )
  
  return render_template('annotations.html', annotations=response["Items"])


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    current_user_id = session['primary_identity']
    # query dynamo for this unique job-id
    response = dynamo_table.query(
      KeyConditionExpression=Key(app.config['AWS_DYNAMODB_PARTITION_KEY']).eq(id)
    )

    # if can't find the job id
    if not response["Items"]:
      return render_template('error.html', 
        title='Page not found', alert_level='warning',
        message="The page you tried to reach does not exist because that JobID doesn't exist. \
          Please check the URL and try again."
      ), 404

    job_data = response["Items"][0]
    requested_user_id = job_data["user_id"]
    s3_key_input_file = job_data["s3_key_input_file"]
    s3_key_result_file = job_data.get("s3_key_result_file")

    # return error if user id in response doesn't match current session user (i.e. not authorized)
    if current_user_id != requested_user_id:
      return render_template('error.html',
        title='Not authorized', alert_level='danger',
        message="You are not authorized to view the details of this job."
        ), 403

    result_is_archived = False
    # If the results file is in the Dynamo entry, the annotation job completed
    if s3_key_result_file is not None:
      # try to generate presigned posts for user to download the results file
      try:
        # check if the S3 file exists
        s3.head_object(Bucket=app.config['AWS_S3_RESULTS_BUCKET'], Key=s3_key_result_file)

        presigned_results_file = s3.generate_presigned_url(
          ClientMethod='get_object',
          Params={
            'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
            'Key': s3_key_result_file,
            'ResponseContentDisposition': 'attachment'
          },
          ExpiresIn=310
        )
      except ClientError as e:
        app.logger.error(f"Unable to generate presigned download URL for input file: {e.response}")
        # if S3 can't find the key, that means this file must have been archived to Glacier at SOME POINT
        if e.response['Error']['Code'] == '404':
          # current user is premium, then the file must currently be in the process of being unarchived bc otherwise it would be in S3
          if session['role'] == "premium_user":
            app.logger.info("This file is currently being restored from Glacier...")
            result_is_archived = True
          presigned_results_file = ""
        else:
          # some other error occurred
          return render_template('error.html',
            title='Error', alert_level='danger',
            message="There was an issue preparing the results file for download."
            ), 403
    else:
      presigned_results_file = ""

    ## https://allwin-raju-12.medium.com/boto3-and-python-upload-download-generate-pre-signed-urls-and-delete-files-from-the-bucket-87b959f7bbaf
    # generate presigned posts for user to download the input file
    try: 
      presigned_input_file = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={
          'Bucket': app.config['AWS_S3_INPUTS_BUCKET'],
          'Key': s3_key_input_file,
          'ResponseContentDisposition': 'attachment'
        },
        ExpiresIn=310
      )
    except ClientError as e:
      app.logger.error(f"Unable to generate presigned download URL for input file: {e}")
      return render_template('error.html',
        title='Error', alert_level='danger',
        message="There was an issue preparing the input file for download."
        ), 403
  
    return render_template('annotation_details.html', annotation=response["Items"][0], download_result_file=presigned_results_file, 
                         download_input_file=presigned_input_file, result_is_archived=result_is_archived)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  current_user_id = session['primary_identity']
  # query dynamo for this unique job-id
  response = dynamo_table.query(
    KeyConditionExpression=Key(app.config['AWS_DYNAMODB_PARTITION_KEY']).eq(id)
  )

  # if can't find the job
  if not response["Items"]:
    return render_template('error.html', 
      title='Page not found', alert_level='warning',
      message="The page you tried to reach does not exist because that JobID doesn't exist. \
        Please check the URL and try again."
    ), 404

  job_data = response["Items"][0]
  job_user_id = job_data["user_id"]
  # return error if user id associated with job doesn't match current session user (i.e. not authorized)
  if current_user_id != job_user_id:
    return render_template('error.html',
      title='Not authorized', alert_level='danger',
      message="You are not authorized to view the results of this job."
      ), 403

  ## https://saturncloud.io/blog/python-aws-boto3-how-to-read-files-from-s3-bucket/
  obj = s3.get_object(Bucket=app.config['AWS_S3_RESULTS_BUCKET'], Key=job_data["s3_key_log_file"])
  data = obj['Body'].read()
  app.logger.info(f"Successfully got log file output for job {id}.")

  return render_template('view_log.html', log_file_contents=data, job_id=id)


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Make sure you handle files not yet archived! -----------------------------------------------------------------------------------
    data_obj = {
      "user_id": session['primary_identity']
    }
    try: 
        response = sns.publish(
            TopicArn=app.config['AWS_SNS_RESTORE_GLACIER_TOPIC'],
            Message=json.dumps(data_obj),
            MessageStructure='string',
        )
        app.logger.info(f"Successfully initiated Glacier archive restoration for user {session['primary_identity']}.")
    except ParamValidationError as e:
        app.logger.error(f"Could not post Glacier archive restoration request to SNS: {e}")
        abort(500)
    except KeyError as e:
        app.logger.error(f"Could not post Glacier archive restoration request to SNS: {e}")
        abort(500)

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))



"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500
