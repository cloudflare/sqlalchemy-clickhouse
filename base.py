#!/usr/bin/env python
#
# Note: parts of the file come from https://github.com/snowflakedb/snowflake-sqlalchemy
#       licensed under the same Apache 2.0 License

import re

import sqlalchemy.types as sqltypes
from sqlalchemy import exc as sa_exc
from sqlalchemy import util as sa_util
from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import compiler, expression
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.dialects.postgresql.base import PGCompiler, PGIdentifierPreparer
from sqlalchemy.types import (
    CHAR, DATE, DATETIME, INTEGER, SMALLINT, BIGINT, DECIMAL, TIME,
    TIMESTAMP, VARCHAR, BINARY, BOOLEAN, FLOAT, REAL)

# Export connector version
VERSION = (0, 1, 0, None)

# Column spec
colspecs = {}

# Type decorators
class ARRAY(sqltypes.TypeEngine):
    __visit_name__ = 'ARRAY'

# Type converters
ischema_names = {
    'Int64': INTEGER,
    'Int32': INTEGER,
    'Int16': INTEGER,
    'Int8': INTEGER,
    'UInt64': INTEGER,
    'UInt32': INTEGER,
    'UInt16': INTEGER,
    'UInt8': INTEGER,
    'Date': DATE,
    'DateTime': DATETIME,
    'Float64': FLOAT,
    'Float32': FLOAT,
    'String': VARCHAR,
    'FixedString': VARCHAR,
    'Enum': VARCHAR,
    'Enum8': VARCHAR,
    'Enum16': VARCHAR,
    'Array': ARRAY,
}

class ClickHouseIdentifierPreparer(PGIdentifierPreparer):
    def quote_identifier(self, value):
        """ Never quote identifiers. """
        return self._escape_identifier(value)
    def quote(self, ident, force=None):
        if self._requires_quotes(ident):
            return '"{}"'.format(ident)
        return ident

class ClickHouseCompiler(PGCompiler):
    def visit_count_func(self, fn, **kw):
        return 'count()'

    def visit_random_func(self, fn, **kw):
        return 'rand()'

    def visit_now_func(self, fn, **kw):
        return 'now()'

    def visit_current_date_func(self, fn, **kw):
        return 'today()'

    def visit_true(self, element, **kw):
        return '1'

    def visit_false(self, element, **kw):
        return '0'

    def visit_cast(self, cast, **kwargs):
        if self.dialect.supports_cast:
            return super(ClickHouseCompiler, self).visit_cast(cast, **kwargs)
        else:
            return self.process(cast.clause, **kwargs)

    def visit_substring_func(self, func, **kw):
        s = self.process(func.clauses.clauses[0], **kw)
        start = self.process(func.clauses.clauses[1], **kw)
        if len(func.clauses.clauses) > 2:
            length = self.process(func.clauses.clauses[2], **kw)
            return "substring(%s, %s, %s)" % (s, start, length)
        else:
            return "substring(%s, %s)" % (s, start)

    def visit_concat_op_binary(self, binary, operator, **kw):
        return "concat(%s, %s)" % (self.process(binary.left), self.process(binary.right))

    def visit_in_op_binary(self, binary, operator, **kw):
        kw['literal_binds'] = True
        return '%s IN %s' % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw)
        )

    def visit_notin_op_binary(self, binary, operator, **kw):
        kw['literal_binds'] = True
        return '%s NOT IN %s' % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw)
        )

    def visit_column(self, column, add_to_result_map=None,
                     include_table=True, **kwargs):
        # Columns prefixed with table name are not supported
        return super(ClickHouseCompiler, self).visit_column(column,
            add_to_result_map=add_to_result_map, include_table=False, **kwargs)

    def render_literal_value(self, value, type_):
        value = super(ClickHouseCompiler, self).render_literal_value(value, type_)
        if isinstance(type_, sqltypes.DateTime):
            value = 'toDateTime(%s)' % value
        if isinstance(type_, sqltypes.Date):
            value = 'toDate(%s)' % value
        return value

    def limit_clause(self, select, **kw):
        text = ''
        if select._limit_clause is not None:
            text += '\n LIMIT ' + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            text = '\n LIMIT '
            if select._limit_clause is None:
                text += self.process(sql.literal(-1))
            else:
                text += '0'
            text += ',' + self.process(select._offset_clause, **kw)
        return text

    def for_update_clause(self, select, **kw):
        return '' # Not supported

class ClickHouseExecutionContext(default.DefaultExecutionContext):
    @sa_util.memoized_property
    def should_autocommit(self):
        return False # No DML supported, never autocommit

class ClickHouseTypeCompiler(compiler.GenericTypeCompiler):
    def visit_ARRAY(self, type, **kw):
        return "Array(%s)" % type

class ClickHouseDialect(default.DefaultDialect):
    name = 'clickhouse'
    supports_cast = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_native_decimal = True
    supports_native_boolean = True
    supports_alter = True
    supports_sequences = False
    supports_native_enum = True

    max_identifier_length = 127
    default_paramstyle = 'pyformat'
    colspecs = colspecs
    ischema_names = ischema_names
    convert_unicode = True
    returns_unicode_strings = True
    description_encoding = None
    postfetch_lastrowid = False

    preparer = ClickHouseIdentifierPreparer
    type_compiler = ClickHouseTypeCompiler
    statement_compiler = ClickHouseCompiler
    execution_ctx_cls = ClickHouseExecutionContext

    # Required for PG-based compiler
    _backslash_escapes = True

    @classmethod
    def dbapi(cls):
        try:
            import sqlalchemy_clickhouse.connector as connector
        except:
            import connector
        return connector

    def create_connect_args(self, url):
        kwargs = {
            'db_url': 'http://%s:%d/' % (url.host, url.port or 8123),
            'username': url.username,
            'password': url.password,
        }
        kwargs.update(url.query)
        return ([url.database or 'default'], kwargs)

    def get_schema_names(self, connection, **kw):
        return [row.name for row in connection.execute('SHOW DATABASES')]

    def get_view_names(self, connection, schema=None, **kw):
        return self.get_table_names(connection, schema, **kw)

    def _get_table_columns(self, connection, table_name, schema):
        full_table = table_name
        if schema:
            full_table = schema + '.' + table_name
        # This needs the table name to be unescaped (no backticks).
        return connection.execute('DESCRIBE TABLE {}'.format(full_table)).fetchall()

    def has_table(self, connection, table_name, schema=None):
        full_table = table_name
        if schema:
            full_table = schema + '.' + table_name
        for r in connection.execute('EXISTS TABLE {}'.format(full_table)):
            if r.result == 1:
                return True
        return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        rows = self._get_table_columns(connection, table_name, schema)
        result = []
        for r in rows:
            col_name = r.name
            col_type = ""
            if r.type.startswith("AggregateFunction"):
                # Extract type information from a column
                # using AggregateFunction
                # the type from clickhouse will be 
                # AggregateFunction(sum, Int64) for an Int64 type
                # remove first 24 chars and remove the last one to get Int64
                col_type = r.type[23:-1]
            else:    
                # Take out the more detailed type information
                # e.g. 'map<int,int>' -> 'map'
                #      'decimal(10,1)' -> decimal                
                col_type = re.search(r'^\w+', r.type).group(0)
            try:
                coltype = ischema_names[col_type]
            except KeyError:
                coltype = sqltypes.NullType
            result.append({
                'name': col_name,
                'type': coltype,
                'nullable': True,
                'default': None,
            })
        return result

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # No support for foreign keys.
        return []

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # No support for primary keys.
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        full_table = table_name
        if schema:
            full_table = schema + '.' + table_name
        # We must get the full table creation STMT to parse engine and partitions
        rows = [r for r in connection.execute('SHOW CREATE TABLE {}'.format(full_table))]
        if len(rows) < 1:
            return []
        # VIEWs are not going to have ENGINE associated, there is no good way how to
        # determine partitioning columns (or indexes)
        engine_spec = re.search(r'ENGINE = (\w+)\((.+)\)', rows[0].statement)
        if not engine_spec:
            return []
        engine, params = engine_spec.group(1,2)
        # Handle partition columns
        cols = re.search(r'\((.+)\)', params)
        if not cols:
            return []
        col_names = [c.strip() for c in cols.group(1).split(',')]
        return [{'name': 'partition', 'column_names': col_names, 'unique': False}]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        query = 'SHOW TABLES'
        if schema:
            query += ' FROM ' + schema
        return [row.name for row in connection.execute(query)]

    def do_rollback(self, dbapi_connection):
        # No transactions
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # We decode everything as UTF-8
        return True

    def _check_unicode_description(self, connection):
        # We decode everything as UTF-8
        return True

dialect = ClickHouseDialect