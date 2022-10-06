from sommelier.metrics_table import MetricsTable


class MockTableQuery(MetricsTable):
    table_name = 'fake_table'
    dimensions = {
        'flight_number': str,
        'airport': str,
        'model': str
    }
    metrics = {
        'price': int,
        'distance': int
    }
    other = {
        'date': str
    }
    date_column = 'date'


def test_select_all_dimensions_and_metrics():
    """
    Ensure "*" is returned for selected column if all metrics and dimensions are selected
    """
    query = MockTableQuery()
    query.select_all_dimensions()
    query.select_all_metrics()

    assert ['*'] == query._selected_column_strings()

    query = MockTableQuery()
    query.select_all_dimensions()
    assert ['*'] != query._selected_column_strings()


def test_select_all_dimensions():
    """
    Ensure all the dimension columns are added to the selected list
    """
    query = MockTableQuery()
    query.select_all_dimensions()
    assert len(query._selected) == 3
    assert 'flight_number' in query._selected
    assert 'airport' in query._selected
    assert 'model' in query._selected


def test_select_all_metrics():
    """
    Ensure all the metrics columns are added to the selected list
    """
    query = MockTableQuery()
    query.select_all_metrics()
    assert len(query._selected) == 2
    assert 'price' in query._selected
    assert 'distance' in query._selected


def test_filter_date_between():
    query = MockTableQuery()
    query.select_all_dimensions()
    query.filter_dates_between('20180101')
    assert 'date' in query.filters
    assert query.filters['date'][0]['op'] == '>='

    query = MockTableQuery()
    query.select_all_dimensions()
    query.filter_dates_between(None, '20180101')
    assert query.filters['date'][0]['op'] == '<='

    query = MockTableQuery()
    query.select_all_dimensions()
    query.filter_dates_between('20180101', '20180106')
    assert 'date' in query.filters
    assert query.filters['date'][0]['op'] == 'between'
    sql = query.get_sql_query()
    assert '"date">=\'20180101\'' in sql
    assert '"date"<=\'20180106\'' in sql


def test_parse_bulk_filters():
    """
    Verify the output of tuples have the correct value for the input
    """
    test_input = {
        'airport': 'sfo',
        'flight_number': {
            'value': ['UA12', 'UA456'],
            'op': 'in'
        },
        'model': {
            'value': 'B787',
            'op': '!='
        }
    }

    parsed = MockTableQuery.parse_bulk_filters(test_input)
    for column, value, op in parsed:
        assert column in ['flight_number', 'airport', 'model']

        if column == 'airport':
            assert value == 'sfo'
            assert op == '=='
        elif column == 'country':
            assert value == ['UA12', 'UA456']
            assert op == 'in'
        elif column == 'region':
            assert value == 'B787'
            assert op == '!='


def test_parse_bulk_filters_list():
    test_input = {
        'browser': [{
            'value': 4,
            'op': '!='
        }, {
            'value': [1, 5],
            'op': 'bt'
        }]
    }

    parsed = MockTableQuery.parse_bulk_filters(test_input)
    assert len(parsed) == 2
    assert parsed[0][2] == '!='
    assert parsed[1][2] == 'bt'
