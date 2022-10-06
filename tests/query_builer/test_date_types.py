import pytest
import os
import time

from sommelier.query_builder.date_types import DateTypes, convert_date_to_type

MS_SINCE_EPOCH_20200303 = 1583193600000
MINUTES_SINCE_EPOCH_20200303 = 26386560


@pytest.fixture(scope='session', autouse=True)
def force_utc_timezone(request):
    """
    Ensure the machine is running the tests in UTC timezone which is what the remote machines are running on
    """
    os.environ['TZ'] = 'UTC'
    time.tzset()


@pytest.mark.parametrize('from_value, from_type, to_type, expected', (
        (MS_SINCE_EPOCH_20200303, DateTypes.MILLISECONDS_SINCE_EPOCH, DateTypes.MILLISECONDS_SINCE_EPOCH, MS_SINCE_EPOCH_20200303),
        (MS_SINCE_EPOCH_20200303, DateTypes.MILLISECONDS_SINCE_EPOCH, DateTypes.SECONDS_SINCE_EPOCH, MS_SINCE_EPOCH_20200303 / 1000),
        (MS_SINCE_EPOCH_20200303, DateTypes.MILLISECONDS_SINCE_EPOCH, DateTypes.MINUTES_SINCE_EPOCH, MINUTES_SINCE_EPOCH_20200303),
        (MS_SINCE_EPOCH_20200303, DateTypes.MILLISECONDS_SINCE_EPOCH, DateTypes.HOURS_SINCE_EPOCH, MINUTES_SINCE_EPOCH_20200303 / 24),
        (MS_SINCE_EPOCH_20200303, DateTypes.MILLISECONDS_SINCE_EPOCH, DateTypes.YYYYMMDD, 20200303),
        (MINUTES_SINCE_EPOCH_20200303, DateTypes.MINUTES_SINCE_EPOCH, DateTypes.MINUTES_SINCE_EPOCH, MINUTES_SINCE_EPOCH_20200303),
        (MINUTES_SINCE_EPOCH_20200303, DateTypes.MINUTES_SINCE_EPOCH, DateTypes.YYYYMMDD, 20200303),
))
def test_convert_date_to_type(from_value: int, from_type: int, to_type: int, expected):
    assert convert_date_to_type(from_value, from_type, to_type) == expected
