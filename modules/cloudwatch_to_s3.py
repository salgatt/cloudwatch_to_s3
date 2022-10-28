import boto3
import os
import datetime
import logging
import time

"""
This portion will receive the n_days value (the date/day of the log you want
want to export) and calculate the start and end date of logs you want to
export to S3. Today = 0; yesterday = 1; so on and so forth...
Ex: If today is April 13th and NDAYS = 0, April 13th logs will be exported.
Ex: If today is April 13th and NDAYS = 1, April 12th logs will be exported.
Ex: If today is April 13th and NDAYS = 2, April 11th logs will be exported.
"""
def generate_date_dict(n_days = 1):
    currentTime = datetime.datetime.now()
    date_dict = {
        "start_date" : currentTime - datetime.timedelta(days=n_days),
        "end_date" : currentTime - datetime.timedelta(days=n_days - 1),
    }
    return date_dict

"""
Convert the from & to Dates to milliseconds
"""
def convert_from_date(date_time = datetime.datetime.now()):
    return int(date_time.timestamp() * 1000)


"""
The following will create the subfolders' structure based on year, month, day
Ex: BucketNAME/LogGroupName/Year/Month/Day
"""
def generate_prefix(s3_bucket_prefix, start_date):
    return os.path.join(s3_bucket_prefix, start_date.strftime('%Y{0}%m{0}%d').format(os.path.sep))


"""
Based on the AWS boto3 documentation
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html#CloudWatchLogs.Client.create_export_task
"""
def create_export_task(group_name, from_date, to_date, s3_bucket_name, s3_bucket_prefix):
    logging.info("Creating CloudWatch Log Export Task to S3")
    try:
        client = boto3.client('logs')
        response = client.create_export_task(
             logGroupName=group_name,
             fromTime=from_date,
             to=to_date,
             destination=s3_bucket_name,
             destinationPrefix=s3_bucket_prefix
            )
        if 'taskId' not in response:
            logging.error("Unexpected createExportTask response")
            raise Exception("Unexpected createExportTask response")
        return response['taskId']
    except Exception as e:
        logging.error(e)
        raise Exception("Failed to Create Export Task")

"""
Based on the AWS boto3 documentation
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html#CloudWatchLogs.Client.describe_export_tasks
"""
def export_status_call(task_id):
    try:
        client = boto3.client('logs')
        response = client.describe_export_tasks(
             taskId = task_id
            )

        if 'exportTasks' not in response:
            logging.error("Unexpected describeExportTasks response")
            raise Exception("Unexpected describeExportTasks response")

        if len(response['exportTasks']) != 1:
            logging.error("Wrong number of export tasks found")
            raise Exception("Wrong number of export tasks found")

        if "status" not in response['exportTasks'][0]:
            logging.error("No status found in describeExportTasks response")
            raise Exception("No status found in describeExportTasks response")

        if "code" not in response['exportTasks'][0]['status']:
            logging.error("No status code found in describeExportTasks response")
            raise Exception("No status code found in describeExportTasks response")

        if response['exportTasks'][0]['status']['code'] == 'RUNNING':
            return "RUNNING"

        if response['exportTasks'][0]['status']['code'] == 'COMPLETED':
            return "COMPLETED"

        # Otherwise we should be in a failed state
        logging.error("Task is in an unexpected state")
        raise Exception("Task is in an unexpected state")

    except Exception as e:
        logging.error(e)
        raise Exception("Status Call Failed")

def check_export_status(task_id, delay = 1):
    logging.info("Checking Status of Export Task")
    try:
        response = export_status_call(task_id = task_id)

        # We want to be sure we're not beating up the API and we'll increase our delay until the task completes
        while(response == 'RUNNING'):
            logging.warning("We did not get a COMPLETED response so waiting " + str(delay) + " seconds until checking status again")
            time.sleep(delay)
            response = export_status_call(task_id = task_id)
            delay = delay * 2

        logging.info("Log Export Task has completed")
        return True

    except Exception as e:
        logging.error(e)
        raise Exception("Describe Check Failed")

"""
The main function in here
"""
def run(group_name, s3_bucket_name, s3_bucket_prefix, n_days):
    try:
        """
        Based upon what we've been provided for n_days, we'll generate the dates needed to run
        """
        date_dict = generate_date_dict(n_days = n_days)
        date_dict['start_date_ms'] = convert_from_date(date_time = date_dict['start_date'])
        date_dict['end_date_ms'] = convert_from_date(date_time = date_dict['end_date'])
        s3_bucket_prefix_with_date = generate_prefix(s3_bucket_prefix = s3_bucket_prefix, start_date = date_dict['start_date'])
        export_task_id = create_export_task(group_name = group_name, from_date = date_dict['start_date_ms'], to_date = date_dict['end_date_ms'], s3_bucket_name = s3_bucket_name, s3_bucket_prefix = s3_bucket_prefix_with_date)
        logging.debug("Export Task ID : " + export_task_id)
        check_export_status(task_id = export_task_id)
        return {
          "task_id" : export_task_id,
          "s3_export_path" : s3_bucket_prefix_with_date
        }
    except Exception as e:
        logging.error(e)
        raise e

