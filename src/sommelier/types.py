from typing import Dict, Callable

from sommelier.query_builder.date_types import DateField


ColumnTypeDict = Dict[str, Callable]
DateTypeDict = Dict[str, DateField]
