from modules import cloudwatch_to_s3
import logging
import os

"""
This portion will obtain the Environment variables
"""
GROUP_NAME = os.environ['GROUP_NAME']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']
PREFIX = os.environ['PREFIX']
NDAYS = os.environ['NDAYS']
n_days = int(NDAYS)

if __name__ == '__main__':
    try:
        response = cloudwatch_to_s3.run(group_name = GROUP_NAME, s3_bucket_name = DESTINATION_BUCKET, s3_bucket_prefix = PREFIX, n_days = n_days)
        print(response)
    except Exception as e:
        logging.error(e)
        print(e)

