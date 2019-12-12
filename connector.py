#!/usr/bin/env python
#
# See http://www.python.org/dev/peps/pep-0249/
#
# Many docstrings in this file are based on the PEP, which is in the public domain.

from __future__ import absolute_import
from __future__ import unicode_literals
import re
import uuid
import requests
from infi.clickhouse_orm.models import ModelBase
from infi.clickhouse_orm.database import Database

# fix local timezone
from tzlocal import get_localzone
import os
# fixed local timezone

# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
paramstyle = 'pyformat'  # Python extended format codes, e.g. ...WHERE name=%(name)s

# Python 2/3 compatibility
try:
    isinstance(obj, basestring)
except NameError:
    basestring = str

class Error(Exception):
    """Exception that is the base class of all other error exceptions.
    You can use this to catch all errors with one single except statement.
    """
    pass

class ParamEscaper(object):
    def escape_args(self, parameters):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return tuple(self.escape_item(x) for x in parameters)
        else:
            raise Exception("Unsupported param format: {}".format(parameters))

    def escape_number(self, item):
        return item

    def escape_string(self, item):
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode('utf-8')
        return "'{}'".format(item.replace("\\", "\\\\").replace("'", "\\'").replace("$", "$$"))

    def escape_item(self, item):
        if item is None:
            return 'NULL'
        elif isinstance(item, (int, float)):
            return self.escape_number(item)
        elif isinstance(item, basestring):
            return self.escape_string(item)
        else:
            raise Exception("Unsupported object {}".format(item))

_escaper = ParamEscaper()

# Patch ORM library
@classmethod
def create_ad_hoc_field(cls, db_type):
    import infi.clickhouse_orm.fields as orm_fields

    # Enums
    if db_type.startswith('Enum'):
        db_type = 'String' # enum.Eum is not comparable
    # Arrays
    if db_type.startswith('Array'):
        inner_field = cls.create_ad_hoc_field(db_type[6 : -1])
        return orm_fields.ArrayField(inner_field)
    # FixedString
    if db_type.startswith('FixedString'):
        db_type = 'String'

    if db_type == 'LowCardinality(String)':
        db_type = 'String'

    if db_type.startswith('DateTime'):
        db_type = 'DateTime'

    if db_type.startswith('Nullable'):
        inner_field = cls.create_ad_hoc_field(db_type[9 : -1])
        return orm_fields.NullableField(inner_field)
   
    # db_type for Deimal comes like 'Decimal(P, S) string where P is precision and S is scale'
    if db_type.startswith('Decimal'):
        nums = [int(n) for n in db_type[8:-1].split(',')]
        return orm_fields.DecimalField(nums[0], nums[1])
    
    # Simple fields
    name = db_type + 'Field'
    if not hasattr(orm_fields, name):
        raise NotImplementedError('No field class for %s' % db_type)
    return getattr(orm_fields, name)()
ModelBase.create_ad_hoc_field = create_ad_hoc_field

from six import PY3, string_types
def _send(self, data, settings=None, stream=False):
    if PY3 and isinstance(data, string_types):
        data = data.encode('utf-8')
    params = self._build_params(settings)
    r = requests.post(self.db_url, params=params, data=data, stream=stream)
    if r.status_code != 200:
        raise Exception(r.text)
    return r
Database._send = _send

#
# Connector interface
#

def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

class Connection(Database):
    """
        These objects are small stateless factories for cursors, which do all the real work.
    """
    def __init__(self, db_name, db_url='http://localhost:8123/', username=None, password=None, readonly=False):
        super(Connection, self).__init__(db_name, db_url, username, password, readonly)
        self.db_name = db_name
        self.db_url = db_url
        self.username = username
        self.password = password
        self.readonly = readonly

    def close(self):
        pass

    def commit(self):
        pass

    def cursor(self):
        return Cursor(self)

    def rollback(self):
        raise NotSupportedError("Transactions are not supported")  # pragma: no cover

class Cursor(object):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """
    _STATE_NONE = 0
    _STATE_RUNNING = 1
    _STATE_FINISHED = 2

    def __init__(self, database):
        self._db = database
        self._reset_state()
        self._arraysize = 1

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        self._uuid = None
        self._columns = None
        self._rownumber = 0
        # Internal helper state
        self._state = self._STATE_NONE
        self._data = None
        self._columns = None

    @property
    def rowcount(self):
        """By default, return -1 to indicate that this is not supported."""
        return -1

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        # Sleep until we're done or we got the columns
        if self._columns is None:
            return []
        return [
            # name, type_code, display_size, internal_size, precision, scale, null_ok
            (col[0], col[1], None, None, None, None, True) for col in self._columns
        ]

    def close(self):
        pass

    def execute(self, operation, parameters=None, is_response=True):
        """Prepare and execute a database operation (query or command). """
        if parameters is None or not parameters:
            sql = operation
        else:
            sql = operation % _escaper.escape_args(parameters)

        self._reset_state()

        self._state = self._STATE_RUNNING
        self._uuid = uuid.uuid1()

        if is_response:
            response = self._db.select(sql, settings={'query_id': self._uuid})
            self._process_response(response)
        else:
            self._db.raw(sql)

    def executemany(self, operation, seq_of_parameters):
        """Prepare a database operation (query or command) and then execute it against all parameter
        sequences or mappings found in the sequence ``seq_of_parameters``.

        Only the final result set is retained.

        Return values are not defined.
        """
        values_list = []
        RE_INSERT_VALUES = re.compile(
            r"\s*((?:INSERT|REPLACE)\s.+\sVALUES?\s*)" +
            r"(\(\s*(?:%s|%\(.+\)s)\s*(?:,\s*(?:%s|%\(.+\)s)\s*)*\))" +
            r"(\s*(?:ON DUPLICATE.*)?);?\s*\Z",
            re.IGNORECASE | re.DOTALL)

        m = RE_INSERT_VALUES.match(operation)
        if m:
            q_prefix = m.group(1) % ()
            q_values = m.group(2).rstrip()

            for parameters in seq_of_parameters[:-1]:
                values_list.append(q_values % _escaper.escape_args(parameters))
            query = '{} {};'.format(q_prefix, ','.join(values_list))
            return self._db.raw(query)
        for parameters in seq_of_parameters[:-1]:
            self.execute(operation, parameters, is_response=False)

    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence, or ``None`` when
        no more data is available. """
        if self._state == self._STATE_NONE:
            raise Exception("No query yet")
        if not self._data:
            return None
        else:
            self._rownumber += 1
            return self._data.pop(0)

    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a
        list of tuples). An empty sequence is returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If it is not given, the
        cursor's arraysize determines the number of rows to be fetched. The method should try to
        fetch as many rows as indicated by the size parameter. If this is not possible due to the
        specified number of rows not being available, fewer rows may be returned.
        """
        if self._state == self._STATE_NONE:
            raise Exception("No query yet")

        if size is None:
            size = 1

        if not self._data:
            return []
        else:
            if len(self._data) > size:
                result, self._data = self._data[:size], self._data[size:]
            else:
                result, self._data = self._data, []
            self._rownumber += len(result)
            return result

    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as a sequence of sequences
        (e.g. a list of tuples).
        """
        if self._state == self._STATE_NONE:
            raise Exception("No query yet")

        if not self._data:
            return []
        else:
            result, self._data = self._data, []
            self._rownumber += len(result)
            return result

    @property
    def arraysize(self):
        """This read/write attribute specifies the number of rows to fetch at a time with
        :py:meth:`fetchmany`. It defaults to 1 meaning to fetch a single row at a time.
        """
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        self._arraysize = value

    def setinputsizes(self, sizes):
        """Does nothing by default"""
        pass

    def setoutputsize(self, size, column=None):
        """Does nothing by default"""
        pass

    def __next__(self):
        """Return the next row from the currently executing SQL statement using the same semantics
        as :py:meth:`fetchone`. A ``StopIteration`` exception is raised when the result set is
        exhausted.
        """
        one = self.fetchone()
        if one is None:
            raise StopIteration
        else:
            return one

    next = __next__

    def __iter__(self):
        """Return self to make cursors compatible to the iteration protocol."""
        return self

    def cancel(self):
        if self._state == self._STATE_NONE:
            raise ProgrammingError("No query yet")
        if self._uuid is None:
            assert self._state == self._STATE_FINISHED, "Query should be finished"
            return
        # Replace current running query to cancel it
        self._db.select("SELECT 1", settings={"query_id":self._uuid})
        self._state = self._STATE_FINISHED
        self._uuid = None
        self._data = None

    def poll(self):
        pass

    def _process_response(self, response):
        """ Update the internal state with the data from the response """
        assert self._state == self._STATE_RUNNING, "Should be running if processing response"
        cols = None
        data = []
        for r in response:
            if not cols:
                cols = [(f, r._fields[f].db_type) for f in r._fields]
            vals = []
            for fi in r._fields:
                val = getattr(r, fi)

                clickhouseUseLocalTimezone = int(os.getenv('CLICKHOUSE_USE_LOCAL_TIMEZONE', '0'))
                if clickhouseUseLocalTimezone and (r._fields[fi].db_type == 'DateTime' or r._fields[fi].db_type == 'Date'):
                    val = val.astimezone(get_localzone())

                vals.append(val)
            data.append(vals)
            # data.append([getattr(r, f) for f in r._fields])
        self._data = data
        self._columns = cols
        self._state = self._STATE_FINISHED
