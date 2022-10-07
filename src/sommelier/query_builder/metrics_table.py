from sommelier.query_builder.table import Table
from sommelier.types import ColumnTypeDict, DateTypeDict


class MetricsTable(Table):
    """
    This is a base class to help build queries for Pinot dimension + metrics tables. This table is also expected to
    have a column that represents the timestamp for the row. It defaults to "Date".
    Developers are expected to override this

    str table_name - The Pinot table name
    set dimensions - List of string column names
    set metrics - List of string column names
    set other - List of string column names
    """

    def __init__(self, table_name: str,
                 dimension_columns: ColumnTypeDict,
                 metrics_columns: ColumnTypeDict,
                 datetime_columns: DateTypeDict):

        converted_date_columns: ColumnTypeDict = {}

        for datetime_column_name, column_info in datetime_columns.items():
            converted_date_columns[datetime_column_name] = column_info.data_type

        all_columns: ColumnTypeDict = dict(**dimension_columns, **metrics_columns, **converted_date_columns)

        super(MetricsTable, self).__init__(table_name, all_columns)

        self.dimensions: ColumnTypeDict = dimension_columns
        self.metrics: ColumnTypeDict = metrics_columns
        self.datetime_columns: DateTypeDict = datetime_columns

    def _selected_column_strings(self):
        """
        If the all the metrics and dimensions are in the selected return "*" otherwise the selected

        :return: List of string column names
        """
        metrics_and_dimensions = set(self.dimensions.keys()) | set(self.metrics.keys())
        intersection = self._selected.intersection(metrics_and_dimensions)

        if len(metrics_and_dimensions) == len(intersection):
            return ['*']

        return self._selected

    def select_all_dimensions(self):
        """
        Call the select_columns passing the table's dimension attribute

        :return: The current query instance
        """
        return self.select_columns(self.dimensions.keys())

    def select_all_metrics(self):
        """
        Call the select_columns passing the table's metrics attribute

        :return: The current query instance
        """
        return self.select_columns(self.metrics.keys())

    def filter_dates_between(self, start=None, end=None, date_column_override=None):
        """
        Use the first column unless an override is provided

        :param str start: YYYYMMDD
        :param str end: YYYYMMDD
        :param str date_column_override: use this column instead of the default
        :return: The current query instance
        """
        datetime_column = list(self.datetime_columns.keys())[0]

        if date_column_override:
            datetime_column = date_column_override

        if start and end:
            self.filter_column_by_value(datetime_column, [start, end], operator='between')
        else:
            if start:
                self.filter_column_by_value(datetime_column, start, operator='>=')
            if end:
                self.filter_column_by_value(datetime_column, end, operator='<=')

        return self

    @staticmethod
    def parse_bulk_filters(filters):
        """
        Convert dict of filters into list of column name, value, and operator tuples. The operators supported are
        located in the "get_query" function code

        The filters dict is expected to be of the following format:

        {
            'columnName': 'value',
            'columnName2': {
                'value': [1, 2, 3],
                'op': 'in'
            },
            'columnName3': {
                'value': 'all,
                'op': '!='
            },
            'columnName4': [{
                'value': 4,
                'op': '!='
            }, {
                'value': [1, 6],
                'op': 'bt'
            }]
        }

        :param dict filters: Keys are expected to be column names, values are described above
        :return: List of tuples that are column name, value, and operator
        """
        parsed = []
        for column, filter_info in filters.items():
            if isinstance(filter_info, list):
                for config in filter_info:
                    value = config['value']
                    op = config['op']
                    parsed.append((column, value, op))
            else:
                if isinstance(filter_info, dict):
                    value = filter_info['value']
                    op = filter_info['op']
                else:
                    value = filter_info
                    op = '=='
                parsed.append((column, value, op))
        return parsed
