# pinot-sommelier

This library helps you build programmatically build queries for apache pinot.

## Usage

Install `pinot-sommelier`:

```
$ pip install pinot-sommelier
```

Then in your project simply use:

```python
from sommelier.query_builder.metrics_table import MetricsTable

query = MetricsTable(
    table_name='fake_table',
    dimension_columns={
        'flight_number': str,
        'airport': str,
        'model': str
    },
    metrics_columns={
        'price': int,
        'distance': int
    },
    date_column='date')

query.select_all_columns()
query.group_by(['flight_number', 'model'])
query.select('sum(price)')
query.filter_column_by_value('flight_number', 'UA111')

print(query.get_sql_query())
# "SELECT * FROM "fake_table" WHERE "flight_number"='UA111' GROUP BY "flight_number","model""
```