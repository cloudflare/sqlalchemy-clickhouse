from sqlalchemy.sql.functions import GenericFunction

class with_cube(GenericFunction):
    r"""Implement the ``WITH CUBE`` grouping operation.

    This function is used as part of the GROUP BY of a statement,
    e.g. :meth:`.Select.group_by`::

        stmt = select(
            [func.sum(table.c.value), table.c.col_1, table.c.col_2]
        ).group_by(table.c.col_1, table.c.col_2, func.with_cube())
    """

class with_rollup(GenericFunction):
    r"""Implement the ``WITH ROLLUP`` grouping operation.

    This function is used as part of the GROUP BY of a statement,
    e.g. :meth:`.Select.group_by`::

        stmt = select(
            [func.sum(table.c.value), table.c.col_1, table.c.col_2]
        ).group_by(table.c.col_1, table.c.col_2, func.with_rollup())
    """

class with_totals(GenericFunction):
    r"""Implement the ``WITH TOTALS`` grouping operation.

    This function is used as part of the GROUP BY of a statement,
    e.g. :meth:`.Select.group_by`::

        stmt = select(
            [func.sum(table.c.value), table.c.col_1, table.c.col_2]
        ).group_by(table.c.col_1, table.c.col_2, func.with_totals())
    """
