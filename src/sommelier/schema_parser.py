from sommelier.types import ColumnTypeDict, DateTypeDict

pinot_type_to_python_type = {
    'INT': int,
    'LONG': int,
    'FLOAT': float,
    'DOUBLE': float,
    'BIG_DECIMAL': float,
    'BOOLEAN': bool,
    'TIMESTAMP': str,
    'STRING': str,
    'JSON': str,
    'BYTES': bytes,
}


def get_table_information_from_schema(schema_configuration):
    """
    See reference at https://docs.pinot.apache.org/basics/components/schema

    :param schema_configuration: Configuration JSON but a dict
    :return: Tuple of dimensions, metrics, and time columns information to be used in a MetricsTable instantiation
    """
    metrics: ColumnTypeDict = {}
    dimensions: ColumnTypeDict = {}
    time_columns: DateTypeDict = {}

    if 'dimensionFieldSpecs' in schema_configuration:
        for dimension_config in schema_configuration['dimensionFieldSpecs']:
            dimensions[dimension_config['name']] = pinot_type_to_python_type[dimension_config['dataType']]

    if 'metricFieldSpecs' in schema_configuration:
        for metric_config in schema_configuration['metricFieldSpecs']:
            metrics[metric_config['name']] = pinot_type_to_python_type[metric_config['dataType']]

    if 'dateTimeFieldSpecs' in schema_configuration:
        for time_config in schema_configuration['dateTimeFieldSpecs']:
            time_columns[time_config['name']] = {
                'data_type': pinot_type_to_python_type[time_config['dataType']],
                'format': time_config['format'],
                'granularity': time_config['granularity']
            }

    return dimensions, metrics, time_columns
