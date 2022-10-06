from sommelier.schema_parser import get_table_information_from_schema

test_schema = {
    'schemaName': 'flights',
    'dimensionFieldSpecs': [
        {
            'name': 'flightNumber',
            'dataType': 'LONG'
        },
        {
            'name': 'tags',
            'dataType': 'STRING',
            'singleValueField': False,
            'defaultNullValue': 'null'
        }
    ],
    'metricFieldSpecs': [
        {
            'name': 'price',
            'dataType': 'DOUBLE',
            'defaultNullValue': 0
        }
    ],
    'dateTimeFieldSpecs': [
        {
            'name': 'millisSinceEpoch',
            'dataType': 'LONG',
            'format': '1:MILLISECONDS:EPOCH',
            'granularity': '15:MINUTES'
        },
        {
            'name': 'hoursSinceEpoch',
            'dataType': 'INT',
            'format': '1:HOURS:EPOCH',
            'granularity': '1:HOURS'
        },
        {
            'name': 'dateString',
            'dataType': 'STRING',
            'format': '1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd',
            'granularity': '1:DAYS'
        }
    ]
}


def test_get_table_information_from_schema():
    dimensions, metrics, time_columns = get_table_information_from_schema(test_schema)

    assert len(dimensions) == 2
    assert len(metrics) == 1
    assert len(time_columns) == 3

    assert metrics["price"] == float
    assert dimensions["flightNumber"] == int
