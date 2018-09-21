from sqlalchemy import column, func, Integer, select, String, table
from sqlalchemy.exc import CompileError

from test.testcase import BaseTestCase

tbl = table(
    'nba',
    column('id', Integer),
    column('name', String),
    column('city', String),
    column('wins', Integer)
)

class GroupBySummariesTestCase(BaseTestCase):
    def test_group_by_without_summary(self):
        stmt = select([tbl.c.id, tbl.c.name, tbl.c.city]).group_by(
                  tbl.c.id, tbl.c.name, tbl.c.city)
        self.assertEqual(
            self.compile(stmt),
            'SELECT id, name, city FROM nba GROUP BY id, name, city'
        )

    def test_group_by_with_rollup(self):
        stmt = select([tbl.c.id]).group_by(tbl.c.id, func.with_rollup())
        self.assertEqual(
            self.compile(stmt),
            'SELECT id FROM nba GROUP BY id WITH ROLLUP'
        )

    def test_group_by_with_rollup_with_totals(self):
        stmt = select([tbl.c.id]).group_by(
          tbl.c.id, func.with_rollup(), func.with_totals()
        )
        self.assertEqual(
            self.compile(stmt),
            'SELECT id FROM nba GROUP BY id WITH ROLLUP WITH TOTALS'
        )

    def test_group_by_rollup(self):
        stmt = select([tbl.c.id]).group_by(
          func.rollup(tbl.c.id, tbl.c.name)
        )
        self.assertEqual(
            self.compile(stmt),
            'SELECT id FROM nba GROUP BY ROLLUP(id, name)'
        )

    def test_group_by_cube(self):
      stmt = select([tbl.c.id, tbl.c.name, tbl.c.city]).group_by(
        func.cube(tbl.c.id, tbl.c.name, tbl.c.city)
      )
      self.assertEqual(
          self.compile(stmt),
          'SELECT id, name, city FROM nba GROUP BY CUBE(id, name, city)'
      )

    def test_group_by_with_cube_with_totals(self):
      stmt = select(
        [tbl.c.id, tbl.c.name, tbl.c.city, func.SUM(tbl.c.wins).label('wins')]
      ).group_by(
        tbl.c.id, tbl.c.name, tbl.c.city, func.with_cube(), func.with_totals()
      )
      self.assertEqual(
          self.compile(stmt),
          'SELECT id, name, city, SUM(wins) AS wins FROM nba GROUP BY id, name,'
          ' city WITH CUBE WITH TOTALS'
      )

class LimitClauseTestCase(BaseTestCase):
    def test_limit(self):
        stmt = select([tbl.c.id, tbl.c.name]).limit(10)
        self.assertEqual(
            self.compile(stmt, literal_binds=True),
            'SELECT id, name FROM nba  LIMIT 10'
        )

    def test_limit_with_offset(self):
        stmt = select([tbl.c.id, tbl.c.name]).limit(10).offset(5)
        self.assertEqual(
            self.compile(stmt, literal_binds=True),
            'SELECT id, name FROM nba  LIMIT 5, 10'
        )

    def test_offset_without_limit(self):
        stmt = select([tbl.c.id, tbl.c.name]).offset(5)
        with self.assertRaises(CompileError) as ctx:
            self.compile(stmt, literal_binds=True)
