from typing import Dict, Callable, TypedDict


class DateColumnInfo(TypedDict):
    format: str
    data_type: Callable
    granularity: str


ColumnTypeDict = Dict[str, Callable]
DateTypeDict = Dict[str, DateColumnInfo]
