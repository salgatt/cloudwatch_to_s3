import datetime
import logging
import os

# import pytest
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
