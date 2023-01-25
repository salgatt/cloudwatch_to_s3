import datetime
import logging
import os
import re

import boto3
import pytest
from moto import mock_logs, mock_s3

import cloudwatch_to_s3


class TestCloudWatch:
    LOGGER = logging.getLogger(__name__)

    def test_generate_date_dict(self):
        currentTime = datetime.datetime.now()
        start_date = currentTime - datetime.timedelta(days=1)
        end_date = currentTime - datetime.timedelta(days=1 - 1)
        result = cloudwatch_to_s3.generate_date_dict(1)
        assert start_date.strftime("%m/%d/%Y, %H:%M") == result["start_date"].strftime(
            "%m/%d/%Y, %H:%M"
        ) and end_date.strftime("%m/%d/%Y, %H:%M") == result["end_date"].strftime(
            "%m/%d/%Y, %H:%M"
        )

    def test_convert_from_date(self):
        currentTime = datetime.datetime.now()
        expected = int(currentTime.timestamp() * 1000)
        result = cloudwatch_to_s3.convert_from_date(currentTime)
        assert result == expected

    def test_generate_prefix(self):
        currentTime = datetime.datetime.now()
        expected = os.path.join(
            "test_name", currentTime.strftime("%Y{0}%m{0}%d").format(os.path.sep)
        )
        result = cloudwatch_to_s3.generate_prefix("test_name", currentTime)
        assert result == expected

    @mock_logs
    def test_create_export_task_exception(self):
        with pytest.raises(Exception, match="Failed to Create Export Task"):
            cloudwatch_to_s3.create_export_task(
                group_name="group_name",
                from_date="start_date_ms",
                to_date="end_date_ms",
                s3_bucket_name="s3_bucket_name",
                s3_bucket_prefix="s3_bucket_prefix_with_date",
            )

    @mock_logs
    @mock_s3
    def test_create_export_task_success(self):
        conn = boto3.resource("s3", region_name="us-east-1")
        # We need to create the bucket since this is all in Moto's 'virtual' AWS account
        conn.create_bucket(Bucket="s3_bucket_name")

        client = boto3.client("logs", region_name="us-east-1")
        client.create_log_group(logGroupName="group_name")
        date_dict = cloudwatch_to_s3.generate_date_dict(n_days=2)
        date_dict["start_date_ms"] = cloudwatch_to_s3.convert_from_date(
            date_time=date_dict["start_date"]
        )
        date_dict["end_date_ms"] = cloudwatch_to_s3.convert_from_date(
            date_time=date_dict["end_date"]
        )
        s3_bucket_prefix_with_date = cloudwatch_to_s3.generate_prefix(
            s3_bucket_prefix="s3_bucket_prefix", start_date=date_dict["start_date"]
        )
        export_task_id = cloudwatch_to_s3.create_export_task(
            group_name="group_name",
            from_date=date_dict["start_date_ms"],
            to_date=date_dict["end_date_ms"],
            s3_bucket_name="s3_bucket_name",
            s3_bucket_prefix=s3_bucket_prefix_with_date,
        )

        assert re.search(
            "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}",
            export_task_id,
        )
