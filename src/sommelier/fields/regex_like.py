from pypika.terms import Function, Field
from pypika.utils import format_quotes


class RegexLike(Function):
    """
    This allows the sql support of the "regex_like(column, '^pattern')" filter
    """

    def __init__(self, column: Field, pattern: str, **kwargs):
        super(RegexLike, self).__init__(kwargs.get('alias'))
        self.column = column
        self.pattern = pattern

    def get_sql(self,
                **kwargs):
        formatted_pattern = format_quotes(self.pattern, '\'')
        return f'regexp_like({self.column.name}, {formatted_pattern})'
