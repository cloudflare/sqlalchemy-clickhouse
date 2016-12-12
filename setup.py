#!/usr/bin/env python

from os import path, getenv
from setuptools import setup
from codecs import open

VERSION = [0, 1, 0]
readme = open('README.rst').read()

setup(
    name='sqlalchemy-clickhouse',
    version='.'.join('%d' % v for v in VERSION[0:3]) + "-3",
    description='ClickHouse SQLAlchemy Dialect',
    long_description = readme,
    author = 'Cloudflare, Inc.',
    author_email = 'mvavrusa@cloudflare.com',
    license = 'Apache License, Version 2.0',
    url = 'https://github.com/cloudflare/sqlalchemy-clickhouse',
    keywords = "db database cloud analytics clickhouse",
    download_url = 'https://github.com/cloudflare/sqlalchemy-clickhouse/releases/tag/v0.1',
    install_requires = [
        'sqlalchemy>=1.0.0',
        'infi.clickhouse_orm>=0.7.1'
    ],
    packages=[
        'sqlalchemy_clickhouse',
    ],
    package_dir={
        'sqlalchemy_clickhouse': '.',
    },
    package_data={
        'sqlalchemy_clickhouse': ['LICENSE.txt'],
    },
    entry_points={
        'sqlalchemy.dialects': [
            'clickhouse=sqlalchemy_clickhouse.base',
        ]
    },
    classifiers = [
        'Development Status :: 5 - Production/Stable',

        'Environment :: Console',
        'Environment :: Other Environment',

        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',

        'License :: OSI Approved :: Apache Software License',

        'Operating System :: OS Independent',

        'Programming Language :: SQL',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',

        'Topic :: Database',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
