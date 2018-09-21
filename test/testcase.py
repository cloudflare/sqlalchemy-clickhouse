# Attribution: https://github.com/xzkostyan/clickhouse-sqlalchemy
import re
from unittest import TestCase

from sqlalchemy.orm import Query
from base import dialect

class BaseTestCase(TestCase):
    strip_spaces = re.compile(r'[\n\t]')

    def _compile(self, clause, literal_binds=False):
        if isinstance(clause, Query):
            context = clause._compile_context()
            context.statement.use_labels = True
            clause = context.statement

        kw = {}
        if literal_binds:
            kw['compile_kwargs'] = {}
            kw['compile_kwargs']['literal_binds'] = True

        return clause.compile(dialect=dialect(), **kw)

    def compile(self, clause, **kwargs):
        return self.strip_spaces.sub(
            '', str(self._compile(clause, **kwargs))
        )
