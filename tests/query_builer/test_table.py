from sommelier.query_builder.table import Table
from pypika import Order


def get_fake_table():
    return Table(table_name='fake_table', columns={
        'flight_number': str,
        'airport': str,
        'model': str
    })


def fake_table_the_sql():
    return Table(table_name='fake_table_the_sql', columns={
        'flight_number': str,
        'airport': str,
        'model': str
    })


def table_with_metric():
    return Table(table_name='metric_table', columns={
        'flight_number': str,
        'airport': str,
        'model': str,
        'price': int
    })


def test_repr():
    """
    Ensure correct string is returned for the __repr__ function. Ensure the correct SQL query and ordering of
    select and where clause is the same
    """
    query = get_fake_table()
    assert str(query) == ''
    query.select_all_columns()
    assert str(query) == 'SELECT airport,flight_number,model FROM fake_table'

    query.filter_column_by_value('flight_number', 'UA188')
    query.filter_column_by_value('model', 'B777')
    assert str(
        query) == 'SELECT airport,flight_number,model FROM fake_table WHERE flight_number=\'UA188\' AND model=\'B777\''


def test_select():
    """
    Also ensure columns aren't duplicated
    """
    query = get_fake_table()

    query.select('airport')
    assert len(query._selected) == 1
    assert 'airport' in query._selected

    # If selected again, should not be duplicated
    query.select('airport')
    assert len(query._selected) == 1


def test_select_function():
    query = table_with_metric()
    query.select('COUNT(*)')
    assert query.get_sql_query() == 'SELECT COUNT(*) FROM metric_table'
    query = table_with_metric()
    query.select('sum(price)')
    assert query.get_sql_query() == 'SELECT SUM(price) FROM metric_table'


def test_select_columns():
    """
    Also ensure columns aren't duplicated
    """
    query = get_fake_table()
    query.select_columns(['model', 'flight_number'])
    assert len(query._selected) == 2
    assert 'model' in query._selected
    assert 'flight_number' in query._selected

    query.select_columns(['flight_number', 'airport', 'model'])
    assert len(query._selected) == 3
    assert 'flight_number' in query._selected
    assert 'airport' in query._selected
    assert 'model' in query._selected


def test_select_columns_with_function():
    query = table_with_metric()
    query.select_columns(['AVG(price)', 'airport'])
    assert query.get_sql_query() == 'SELECT AVG(price),airport FROM metric_table'


def test_select_all_columns():
    """
    Ensure all columns are added to the selected
    """
    query = get_fake_table()
    query.select_all_columns()
    assert len(query._selected) == 3
    assert 'model' in query._selected
    assert 'airport' in query._selected
    assert 'flight_number' in query._selected


def test_filter_column_by_value():
    """
    Test the simple case where just a column and value is passed. Test the case where 3 parameters are passed. Test the case when a
    column passed does not exist in the table
    """
    query = get_fake_table()
    query.filter_column_by_value('flight_number', 'UA101')

    assert 'flight_number' in query.filters
    assert query.filters['flight_number'][0]['op'] == '=='
    assert query.filters['flight_number'][0]['value'] == 'UA101'

    query = get_fake_table()
    query.filter_column_by_value('model', 'A350', operator='!=')

    assert 'model' in query.filters
    assert query.filters['model'][0]['op'] == '!='
    assert query.filters['model'][0]['value'] == 'A350'

    query.filter_column_by_value('should_not_exist', 'B777', operator='!=')
    assert 'should_not_exist' not in query.filters


def test_get_sql_query():
    """
    Test the simple case where just a column and value is passed. Test where 3 parameters are passed. Test when a column passed does not exist in the table
    """
    a = fake_table_the_sql()  # Ensure there are no class property collisions for the _pypika_table property
    a.select_all_columns()
    assert 'fake_table_the_sql' in a.get_sql_query()

    query = get_fake_table()
    query.select_all_columns()
    sql = query.get_sql_query()
    assert 'SELECT' in sql
    assert 'FROM fake_table' in sql

    query.filters = {
        'tests': [{'op': '!=', 'value': 'not_equal'}],
        'test1': [{'op': '>', 'value': '123'}],
        'test2': [{'op': '>=', 'value': '123'}],
        'test3': [{'op': '<', 'value': '455'}],
        'test4': [{'op': 'between', 'value': [1, 2]}],
        'test5': [{'op': '<=', 'value': 455}],
        'test6': [{'op': 'nin', 'value': [4, 8]}],
    }
    sql = query.get_sql_query()
    assert '<>' in sql  # this is the "!="
    assert '>' in sql
    assert '>=' in sql
    assert '<' in sql
    assert '<=' in sql
    assert 'test4>=1' in sql
    assert 'test4<=2' in sql
    assert 'test5<=455' in sql
    assert 'test6 NOT IN (4,8)' in sql

    query.filters = {
        'tests': [{'op': 'in', 'value': [1, 2, 3]}],
        'test1': [{'op': 'isin', 'value': ['all']}]
    }
    sql = query.get_sql_query()
    assert 'tests IN (1,2,3)' in sql
    assert 'test1 IN (\'all\')' in sql


def test_limit_function():
    """
    Confirm limit works as expected
    """
    query = get_fake_table()
    query.select_all_columns()
    query.limit(200)

    sql_str = query.get_sql_query()
    assert 'LIMIT 200' in sql_str

    query.limit(4)
    sql_str = query.get_sql_query()
    assert 'LIMIT 4' in sql_str


def test_group_by_function():
    """
    Test the cases where there is one column and multiple columns. Ensure columns are sorted as well
    """
    query = get_fake_table()
    query.select_all_columns()

    query.group_by(['flight_number'])
    sql_str = query.get_sql_query()
    assert 'GROUP BY flight_number' in sql_str

    query.group_by(['flight_number', 'model'])
    sql_str = query.get_sql_query()
    assert 'GROUP BY flight_number,model' in sql_str


def test_order_by_function():
    query = get_fake_table()
    query.select('Count(*)')
    query.group_by(['flight_number'])
    query.order_by('Count(*)', order=Order.desc)
    assert 'SELECT COUNT(*) FROM fake_table GROUP BY flight_number ORDER BY COUNT(*) DESC' == query.get_sql_query()


def test_custom_filter():
    """
    Test that the custom criterion translates correctly to SQL for one and then multiple
    """
    pypika_table = get_fake_table().get_pypika_table()
    criterion = pypika_table.flight_number == 'UA188'

    query = get_fake_table()
    query.select_all_columns().add_custom_filter(criterion)

    sql_str = query.get_sql_query()
    assert 'WHERE flight_number=\'UA188\'' in sql_str

    query.select_all_columns().add_custom_filter(criterion)
    sql_str = query.get_sql_query()
    assert 'WHERE flight_number=\'UA188\' AND flight_number=\'UA188\'' in sql_str


def test_build_criterion_for_filter():
    """
    Test that the custom criterion translates correctly to SQL for one and then multiple. Also tests an
    invalid operation case
    """
    test_filter = {'flight_number': ['UA199'], 'model': ['B777']}
    criterion = get_fake_table().build_criterion_for_filter(test_filter)
    assert str(criterion) == '"flight_number"=\'UA199\' AND "model"=\'B777\''

    test_filter_2 = {'model': [{'op': '{]', 'value': 'B777'}], 'flight_number': ['UA199']}
    criterion = get_fake_table().build_criterion_for_filter(test_filter_2)
    assert str(criterion) == '"flight_number"=\'UA199\''


def test_regex_like_criterion():
    """
    Ensure the right format is outputted for pql
    """
    test_filter = {'model': [{'op': 'regex', 'value': '^B'}]}
    criterion = get_fake_table().build_criterion_for_filter(test_filter)
    assert str(criterion) == 'regexp_like(model, \'^B\')'


def test_regex_like_integration():
    """
    Ensure the regexp_like function can be used with other filters
    """
    query = get_fake_table()
    query.select_all_columns() \
        .add_custom_filter(get_fake_table().build_criterion_for_filter({'model': [{'op': 'regex', 'value': '^B'}]})) \
        .filter_column_by_value('flight_number', 'UA111')

    sql_str = query.get_sql_query()
    assert 'AND regexp_like(model, \'^B\')' in sql_str
