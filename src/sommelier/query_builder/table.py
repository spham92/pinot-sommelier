from collections import defaultdict
import re
from typing import Dict, Iterable, List, Union

import pypika
from pypika import functions
from pypika.terms import Field, Star

from sommelier.query_builder.fields.regex_like import RegexLike
from sommelier.query_builder.functions import PercentileEst, PercentileTDigest, Percentile, DistinctCount
from sommelier.types import ColumnTypeDict

FIELD_AGGREGATION_PATTERN = re.compile(r'(.+)\((.+)\)\Z')
PERCENTILE_EXTRACTION = re.compile(r'(\D+)(\d+)')


class Table(object):
    """
    This is a base class to help build queries for Pinot tables. The class variable "columns"
    establishes what columns exist for this table.

    str table_name - The Pinot table name
    set columns - List of string column names that exist in the table
    """
    def __repr__(self):
        return self.get_sql_query()

    def __init__(self, table_name: str, columns: ColumnTypeDict):
        self.table_name = table_name
        self.columns = columns

        self._selected = set()
        self.filters = defaultdict(list)
        self.custom_filters = []
        self._group_by = []
        self._order_by = []
        self._order = None
        self._limit = None
        self._pypika_table = None

    def get_pypika_table(self):
        if not self._pypika_table:
            self._pypika_table = pypika.Table(self.table_name)
        return self._pypika_table

    def get_sql_query(self):
        return self.get_query().get_sql(quote_char=None)

    @staticmethod
    def operator_to_criterion(operator: str, column: Field, value: Union[str, int, float]):
        """
        Converts the operator, column, and value into a criterion

        :param str operator: Expected to be one of the eight supported strings
        :param pypika.terms.Field column: This is expected to be a column from ta pypika table.
        :param [int, float, str] value: The value to produce a filter on for the column
        :return: pypika.terms.Criterion or None if the operator is not supported
        """
        if operator == '==':
            return column == value
        elif operator == '!=':
            return column != value
        elif operator == '>':
            return column > value
        elif operator == '>=':
            return column >= value
        elif operator == '<':
            return column < value
        elif operator == '<=':
            return column <= value
        elif operator == 'isin' or operator == 'in':
            return column.isin(value)
        elif operator == 'notin' or operator == 'nin':
            return column.notin(value)
        elif operator == 'between':
            advanced_criterion = column >= value[0]
            advanced_criterion &= column <= value[1]
            return advanced_criterion
        elif operator == 'regex':
            return RegexLike(column, value)
        return None

    def _selected_column_strings(self):
        """
        Expected to return a list of string column names. This is for child classes to override if needed

        :return: List of string column names
        """
        return self._selected

    def get_query(self):
        """
        Use the pypika library to help build the SQL query for Pinot. Sort the selected list and sort the filter
        keys to ensure the same ordering.

        :return: QueryBuilder instance from the pypika lib
        """
        table = self.get_pypika_table()
        fields = sorted(self._selected_column_strings(), key=lambda x: str(x))
        parsed_terms = []
        for field in fields:
            parsed_term = self.generate_term(field)
            if parsed_term:
                parsed_terms.append(parsed_term)

        query = pypika.Query.from_(table).select(*parsed_terms)

        criterion_for_basic_ops = self.build_criterion_for_filter(self.filters)
        if criterion_for_basic_ops:
            query = query.where(criterion_for_basic_ops)

        if self._limit:
            query = query.limit(self._limit)

        if self._group_by:
            query = query.groupby(*self._group_by)

        if self._order_by:
            parsed_order_by = [self.generate_term(field) for field in self._order_by]
            query = query.orderby(order=self._order, *parsed_order_by)

        if self.custom_filters:
            for criterion in self.custom_filters:
                query = query.where(criterion)

        return query

    def group_by(self, column: str):
        """
        Add single columns to list

        :param str column: string names
        :return: The current query instance
        """
        self._group_by.append(column)
        self._group_by = sorted(self._group_by)
        return self

    def group_by_columns(self, columns: List[str]):
        """
        Add the columns to be grouped by

        :param list columns: string names
        :return: The current query instance
        """
        self._group_by = sorted(columns)
        return self

    def order_by(self, *fields: str, order=None):
        """
        SQL ORDER BY keyword

        :param order: DESC or ASC
        :param fields: fields to order by
        :return: Current query instances
        """
        for field in fields:
            self._order_by.append(field)
        self._order = order
        return self

    def limit(self, value: int):
        """
        Add a limit to the query

        :param int value: Number of results to return
        :return: The current query instance
        """
        self._limit = value
        return self

    def generate_term(self, column_string: str):
        """
        Convert string object in Pypika aggregation and Field objects

        :param column_string: string representation of term. i.e. SUM(foo)
        :return: Pypika terms
        """
        matches = FIELD_AGGREGATION_PATTERN.match(column_string)
        if matches and len(matches.groups()) == 2:
            column = matches.groups()[1]
            if column == '*':
                column = Star()
            else:
                column = Field(column)
            function = matches.groups()[0].capitalize()
            term = None
            if hasattr(functions, function):
                function_method = getattr(functions, function)
                term = function_method(column)
            elif function.startswith('Percentile'):
                perc_matches = PERCENTILE_EXTRACTION.match(function)
                if perc_matches and len(perc_matches.groups()) == 2:
                    percentile = perc_matches.groups()[1]
                    percentile_type = perc_matches.groups()[0]
                    if percentile_type == 'Percentile':
                        term = Percentile(column, percentile)
                    elif percentile_type == 'Percentiletdigest':
                        term = PercentileTDigest(column, percentile)
                    elif percentile_type == 'Percentileest':
                        term = PercentileEst(column, percentile)
            elif function == 'Distinctcount':
                term = DistinctCount(column)

            return Field(column_string)
        else:
            if column_string == '*':
                return Star()
            else:
                return Field(column_string)

    def select(self, column: str):
        """
        Add to internal tracked list of columns to select

        :param str column: Name of the column to add to select
        :return: The current query instance
        """
        self._selected.add(column)

        return self

    def select_columns(self, columns: Iterable[str]):
        """
        Iterate over the list of columns and call the "select" function passing the column name

        :param list columns: List of string column names
        :return: The current query instance
        """
        for column in columns:
            self.select(column)
        return self

    def select_all_columns(self):
        """
        Convenience function to select all columns

        :return: The current query instance
        """
        for column in self.columns:
            self.select(column)
        return self

    def filter_column_by_value(self, column: str, value, operator: str = '=='):
        """
        Check if the column exists in the table and then add the filter config to the filters dict

        :param str column: Column name
        :param * value: Can be string, number, or array
        :param str operator: Currently supports "==", "!=", ">", "<", "<=", ">=", "isin", "between"
        :return: The current query instance
        """
        if column in self.columns:
            self.filters[column].append({'op': operator, 'value': value})

        return self

    def add_custom_filter(self, criterion):
        """
        Add the criterion as a custom special case filter since it's built for pypika already.

        If developers need to use an advanced custom filter or need to use a filter that is not yet supported,
        they can use this function.

        Example:

        criteria = [pinot_query_gen_cls.build_criterion_for_filter(dimension_combination) for dimension_combination in dimension_combinations]

        combined_criterion = criteria[0]

        for criterion in criteria[1:]:
            combined_criterion |= criterion

        q.add_custom_filter(combined_criterion).get_sql_query()

        Documentation for ComplexCriterion: http://pypika.readthedocs.io/en/latest/_modules/pypika/terms.html

        :param pypika.terms.ComplexCriterion criterion: Expected to be a criterion that conforms to the Pypika Criterion
        :return: The current query instance
        """
        self.custom_filters.append(criterion)
        return self

    def build_criterion_for_filter(self, column_filters: Dict[str, List[Union[str, Dict[str, str]]]], concatenate_by_and=True):
        """
        Helper function to build a pypika criterion to build a multiple AND clause for a set of columns

        Converts:

         1. {'airline': ['United'], 'model': ['B777']} to Criterion(airline == 'United' & model == 'B777') which is used as table.column.where(airline == 'United' & model == 'B777')
         2. {'airline': [{'op': '!=', 'value': 'United'}], 'model': 'B777'} to Criterion(airline != 'United' & model == 'B777') which is used as
            table.column.where(airline != 'United' & model == 'B777')

        Iterate on sorted keys to ensure the same ordering for the same filters (but in different order) for
        different instances in the where clause.

        :param dict column_filters: Whose keys are expected to be column names and values are strings
        :param bool concatenate_by_and: Indicates whether to concatenate via AND or OR
        :return: pypika.terms.ComplexCriterion
        """
        table = self.get_pypika_table()
        final_criterion = None

        for column in sorted(column_filters):
            table_column = getattr(table, column)
            for filter_value in column_filters[column]:
                if type(filter_value) is dict:
                    operator = filter_value['op']
                    value = filter_value['value']
                else:
                    operator = '=='
                    value = filter_value

                criterion = self.operator_to_criterion(operator, table_column, value)

                if criterion is None:
                    continue

                if final_criterion is None:
                    final_criterion = criterion
                else:
                    if concatenate_by_and:
                        final_criterion &= criterion
                    else:
                        final_criterion |= criterion

        return final_criterion
