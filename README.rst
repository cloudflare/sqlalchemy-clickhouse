sqlalchemy-clickhouse
=====================

ClickHouse dialect for SQLAlchemy.

Installation
------------

The package is installable through PIP::

   pip install sqlalchemy-clickhouse

Usage
-----

The DSN format is similar to that of regular Postgres::

    >>> import sqlalchemy as sa
    >>> sa.create_engine('clickhouse://username:password@hostname:port/database')
    Engine('clickhouse://username:password@hostname:port/database')

It implements a dialect, so there's no user-facing API.

Testing
-------

The dialect can be registered on runtime if you don't want to install it as::

    from sqlalchemy.dialects import registry
    registry.register("clickhouse", "base", "dialect")
