from datetime import datetime
from enum import Enum
from typing import Callable

YYYYMMDD_FORMAT = '%Y%m%d'
MILLISECONDS_IN_SECONDS = 1000


class DateTypes(Enum):
    MILLISECONDS_SINCE_EPOCH = 'MILLISECONDS_SINCE_EPOCH'
    SECONDS_SINCE_EPOCH = 'SECONDS_SINCE_EPOCH'
    MINUTES_SINCE_EPOCH = 'MINUTES_SINCE_EPOCH'
    HOURS_SINCE_EPOCH = 'HOURS_SINCE_EPOCH'
    YYYYMMDD = 'YYYYMMDD'


# Base date will be milliseconds since epoch
CONVERT_TO_BASE_DATE = {
    DateTypes.MILLISECONDS_SINCE_EPOCH: lambda x: x,
    DateTypes.SECONDS_SINCE_EPOCH: lambda x: x * MILLISECONDS_IN_SECONDS,
    DateTypes.MINUTES_SINCE_EPOCH: lambda x: CONVERT_TO_BASE_DATE[DateTypes.SECONDS_SINCE_EPOCH](x) * 60,
    DateTypes.HOURS_SINCE_EPOCH: lambda x: CONVERT_TO_BASE_DATE[DateTypes.MINUTES_SINCE_EPOCH](x) * 24,
    DateTypes.YYYYMMDD: lambda x: int(datetime.strptime(f'{x}', YYYYMMDD_FORMAT).timestamp()) * MILLISECONDS_IN_SECONDS,
}

# Convert milliseconds to desired format
CONVERT_TO_TYPE = {
    DateTypes.MILLISECONDS_SINCE_EPOCH: lambda x: x,
    DateTypes.SECONDS_SINCE_EPOCH: lambda x: x / MILLISECONDS_IN_SECONDS,
    DateTypes.MINUTES_SINCE_EPOCH: lambda x: CONVERT_TO_TYPE[DateTypes.SECONDS_SINCE_EPOCH](x) / 60,
    DateTypes.HOURS_SINCE_EPOCH: lambda x: CONVERT_TO_TYPE[DateTypes.MINUTES_SINCE_EPOCH](x) / 24,
    DateTypes.YYYYMMDD: lambda x: int(datetime.fromtimestamp(x / MILLISECONDS_IN_SECONDS).strftime(YYYYMMDD_FORMAT)),
}


def convert_date_to_type(from_value: int, from_format: DateTypes, to_format: DateTypes) -> int:
    """
    First convert the "from_value" to milliseconds since epoch and then convert that to the desired type

    :param from_value: Value expected to be in the "from_format"
    :param from_format: Expected to be one of the values from DateTypes constant
    :param to_format: Convert millisecond base date to this format. Expected to be one of the values from DateTypes constant
    :return: Converted date time value
    """
    return int(CONVERT_TO_TYPE[to_format](CONVERT_TO_BASE_DATE[from_format](from_value)))


class DateField:
    """
    Class that abstracts the complexity of a date field in Pinot tables
    """

    def __init__(self,
                 name: str,
                 data_type: Callable,
                 date_format: str,
                 granularity: str):
        self.name = name
        self.data_type = data_type
        self.date_format = date_format
        self.granularity = granularity

    def get_convert_clause(self, convert_to: str, alias: str):
        return f'DATETIMECONVERT(\"{self.name}\", \'{self.date_format}\', \'{convert_to}\')'

