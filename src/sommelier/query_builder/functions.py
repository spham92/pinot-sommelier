from pypika import functions
from pypika.terms import Star


class DistinctCount(functions.DistinctOptionFunction):
    # This is different from normal sql where this functionality is part of Count i.e. COUNT( DISTINCT fieldname)
    def __init__(self, param, alias=None):
        is_star = isinstance(param, str) and '*' == param
        super(DistinctCount, self).__init__('DISTINCTCOUNT', Star() if is_star else param, alias=alias)


class PercentileTDigest(functions.AggregateFunction):
    def __init__(self, term, percentile, alias=None):
        # According to pinot docs, this function name is the only one not all caps
        # https://docs.pinot.apache.org/users/user-guide-query/supported-aggregations
        super(PercentileTDigest, self).__init__(f'PercentileTDigest{percentile}', term, alias=alias)


class Percentile(functions.AggregateFunction):
    def __init__(self, term, percentile, alias=None):
        super(Percentile, self).__init__(f'PERCENTILE{percentile}', term, alias=alias)


class PercentileEst(functions.AggregateFunction):
    def __init__(self, term, percentile, alias=None):
        super(Percentile, self).__init__(f'PERCENTILEEST{percentile}', term, alias=alias)
