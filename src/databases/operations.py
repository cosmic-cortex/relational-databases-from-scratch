from itertools import product
from collections import ChainMap
from functools import reduce
from typing import List, Callable

from .tables import Table, columns_in_table


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
    return table_out


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
    return table_out


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
    table_columns = columns_in_table(table)
    table_out = [
        {columns.get(old_name, old_name): record[old_name] for old_name in table_columns}
        for record in table
    ]
    return table_out


def cross_product(**tables) -> Table:
    """
    Constructs the cross product of tables. Each columnn name will be prefixed with
    the source table name.

    Args:
        **tables: Tables for which cross-product is to be taken.

    Returns:
        table_out: Table, cross-product of the tables.
    """

    # prefixing columns with table name
    updated_tables = [
        rename(table, {column: f"{table_name}.{column}" for column in columns_in_table(table)})
        for table_name, table in tables.items()
    ]

    # constructing the cross-product
    table_out = [dict(ChainMap(*rows)) for rows in product(*updated_tables)]
    return table_out


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
    # row prefixer function for
    row_prefixer = lambda row, prefix: {f"{prefix}.{key}": value for key, value in row.items()}

    # determining the pair of rows which satisfy the conditions
    joined_table = [
        {**row_prefixer(row_l, "left"), **row_prefixer(row_r, "right")}
        for row_l, row_r in product(left, right)
        if all([cond(row_l, row_r) for cond in conditions])
    ]

    return joined_table


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
    common_cols = columns_in_table(left).intersection(columns_in_table(right))
    conditions = [lambda x, y: x[col] == y[col] for col in common_cols]
    return theta_join(left, right, conditions)


def _pad_table(table: Table, with_cols: List):
    padding_row = {col: None for col in with_cols}
    return [{**row, **padding_row} for row in table]


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
    left_cols = columns_in_table(left)
    right_cols = columns_in_table(right)

    left = _pad_table(left, right_cols.difference(left_cols))
    right = _pad_table(right, left_cols.difference(right_cols))

    return left + right


def difference(left: Table, right: Table) -> Table:
    """
    Returns the difference of the tables.

    Args:
        left: Table, the table to make difference from.
        right: Table, table to make difference to.

    Returns:
        table_out: Table, union of the input Tables.
    """
    return [record for record in left if record not in right]


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
    return difference(left, difference(left, right))
