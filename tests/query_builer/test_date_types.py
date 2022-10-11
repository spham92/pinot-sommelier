import pytest
import os
import time

from sommelier.query_builder.date_types import DateTypes, convert_date_to_type, DateField

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
        (MS_SINCE_EPOCH_20200303, DateTypes.MILLISECONDS, DateTypes.MILLISECONDS, MS_SINCE_EPOCH_20200303),
        (MS_SINCE_EPOCH_20200303, DateTypes.MILLISECONDS, DateTypes.SECONDS, MS_SINCE_EPOCH_20200303 / 1000),
        (MS_SINCE_EPOCH_20200303, DateTypes.MILLISECONDS, DateTypes.MINUTES, MINUTES_SINCE_EPOCH_20200303),
        (MS_SINCE_EPOCH_20200303, DateTypes.MILLISECONDS, DateTypes.HOURS, MINUTES_SINCE_EPOCH_20200303 / 24),
        (MS_SINCE_EPOCH_20200303, DateTypes.MILLISECONDS, DateTypes.YYYYMMDD, 20200303),
        (MINUTES_SINCE_EPOCH_20200303, DateTypes.MINUTES, DateTypes.MINUTES, MINUTES_SINCE_EPOCH_20200303),
        (MINUTES_SINCE_EPOCH_20200303, DateTypes.MINUTES, DateTypes.YYYYMMDD, 20200303),
))
def test_convert_date_to_type(from_value: int, from_type: int, to_type: int, expected):
    assert convert_date_to_type(from_value, from_type, to_type) == expected


def test_date_field():
    ms_field = DateField(name='foo', data_type=int, date_format='1:MILLISECONDS:EPOCH', granularity='15:MINUTES')
    assert ms_field.get_date_type() == DateTypes.MILLISECONDS

    simple_date_field = DateField(name='foo',
                                  data_type=str,
                                  date_format='1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd',
                                  granularity='1:DAYS')
    assert simple_date_field.get_date_type() == DateTypes.SIMPLE_DATE_FORMAT
    assert simple_date_field.get_simple_date_format() == 'yyyy-MM-dd'
