from itertools import product
from collections import ChainMap
from functools import reduce
from typing import List, Callable

from .tables import Table, _columns_in_table, _prefix_columns, _prefix_row, _remove_duplicates


def select(table: Table, conditions: List[Callable]) -> Table:
    """
    Selects the record in the table which satisfy the conditions.

    Args:
        table: Table
        conditions: List[Callable], a list of functions. Each function takes a record
            from the table as input and returns a boolean.

    Returns:
        table_out: Table with instances satisfying the conditions.
    """
    table_out = [record for record in table if all(cond(record) for cond in conditions)]
    return _remove_duplicates(table_out)


def project(table: Table, columns: List[str]) -> Table:
    """
    Selects the given columns in the table.

    Args:
        table: Table
        columns: List[str], column names to select

    Returns:
        table_out: Table with only the selected columns.
    """
    table_out = [{column: record[column] for column in columns} for record in table]
    return _remove_duplicates(table_out)


def rename(table: Table, columns: dict) -> Table:
    """
    Renames columns in a Table.
    WARNING: rename is destructive. If the new name of a column is an existing column,
    contents will be overwritten!

    Args:
        table: Table, with columns to be renamed.
        columns: dict, with old_name - new_name pairs.

    Returns:
        table_out: Table with renamed columns.
    """
    table_columns = _columns_in_table(table)
    table_out = [
        {columns.get(old_name, old_name): record[old_name] for old_name in table_columns}
        for record in table
    ]
    return _remove_duplicates(table_out)


def cross_product(left: Table, right: Table) -> Table:
    """
    Constructs the cross product of tables. Each columnn name will be prefixed with
    the source table name.

    Args:
        **tables: Tables for which cross-product is to be taken.

    Returns:
        table_out: Table, cross-product of the tables.
    """
    # prefixing columns with table name
    left = _prefix_columns(left, "left")
    right = _prefix_columns(right, "right")

    table_out = [{**row_l, **row_r} for row_l, row_r in product(left, right)]

    return _remove_duplicates(table_out)


def theta_join(left: Table, right: Table, conditions: List[Callable]) -> Table:
    """
    Joins the table according to conditions.

    Args:
        left: Table.
        right: Table.
        conditions: List[Callable], list of conditions to join on. Each condition
            should be a function mapping a tuple of a row from left and right to a Boolean.
            Example: lambda (x, y): x['id'] == y['employee_id']

    Returns:
        joined_table: Table, theta_join of left and right along the conditions.
    """
    # determining the pair of rows which satisfy the conditions
    joined_table = [
        {**_prefix_row(row_l, "left"), **_prefix_row(row_r, "right")}
        for row_l, row_r in product(left, right)
        if all([cond(row_l, row_r) for cond in conditions])
    ]

    return _remove_duplicates(joined_table)


def natural_join(left: Table, right: Table) -> Table:
    """
    Natural join of the left and right tables. It is the same as a theta join with
    the condition that matching columns should be equal.

    Args:
         left: Table.
         right: Table.

    Returns:
        joined_table: Table, natural join of left and right.
    """
    common_cols = _columns_in_table(left).intersection(_columns_in_table(right))
    conditions = [lambda x, y: x[col] == y[col] for col in common_cols]
    joined_table = theta_join(left, right, conditions)
    return _remove_duplicates(joined_table)


def _pad_table(table: Table, with_cols: List):
    padding_row = {col: None for col in with_cols}
    padded_table = [{**row, **padding_row} for row in table]
    return _remove_duplicates(padded_table)


def union(left: Table, right: Table) -> Table:
    """
    Returns the union of the tables.
    Note: this is not the usual set-theoretic union, since duplicates are allowed.

    Args:
        left: Table.
        right: Table.

    Returns:
        table_out: Table, union of the input Tables.
    """
    # padding
    left_cols = _columns_in_table(left)
    right_cols = _columns_in_table(right)

    left = _pad_table(left, right_cols.difference(left_cols))
    right = _pad_table(right, left_cols.difference(right_cols))

    table_out = left + right

    return _remove_duplicates(table_out)


def difference(left: Table, right: Table) -> Table:
    """
    Returns the difference of the tables.

    Args:
        left: Table, the table to make difference from.
        right: Table, table to make difference to.

    Returns:
        table_out: Table, union of the input Tables.
    """
    table_out = [record for record in left if record not in right]
    return _remove_duplicates(table_out)


def intersection(left: Table, right: Table) -> Table:
    """
    Returns the intersection of the tables.
    Note: this does not add more expressive power to our already existing operations.
        Intersection can be written as the repeated application of the difference
        operator.

    Args:
        left: Table.
        right: Table.

    Returns:
        table_out: Table, intersection of the input Tables
    """
    table_out = difference(left, difference(left, right))
    return _remove_duplicates(table_out)
