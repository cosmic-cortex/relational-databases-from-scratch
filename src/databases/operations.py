from itertools import product
from collections import ChainMap
from functools import reduce
from typing import Set, List, Callable

from .tables import _columns_in_table, _prefix_columns, _prefix_row, Row


def select(table: Set[Row], conditions: List[Callable]) -> Set[Row]:
    """
    Selects the record in the table which satisfy the conditions.

    Args:
        table: Set[Row]
        conditions: List[Callable], a list of functions. Each function takes a record
            from the table as input and returns a boolean.

    Returns:
        table_out: Set[Row] with instances satisfying the conditions.
    """
    table_out = {record for record in table if all(cond(record) for cond in conditions)}
    return table_out


def project(table: Set[Row], columns: List[str]) -> Set[Row]:
    """
    Selects the given columns in the table.

    Args:
        table: Set[Row]
        columns: List[str], column names to select

    Returns:
        table_out: Set[Row] with only the selected columns.
    """
    table_out = {Row({column: record[column] for column in columns}) for record in table}
    return table_out


def rename(table: Set[Row], columns: dict) -> Set[Row]:
    """
    Renames columns in a Set[Row].
    WARNING: rename is destructive. If the new name of a column is an existing column,
    contents will be overwritten!

    Args:
        table: Set[Row], with columns to be renamed.
        columns: dict, with old_name - new_name pairs.

    Returns:
        table_out: Set[Row] with renamed columns.
    """
    table_columns = _columns_in_table(table)
    table_out = {
        Row({columns.get(old_name, old_name): record[old_name] for old_name in table_columns})
        for record in table
    }
    return table_out


def cross_product(left: Set[Row], right: Set[Row]) -> Set[Row]:
    """
    Constructs the cross product of tables. Each columnn name will be prefixed with
    the source table name.

    Args:
        **tables: Set[Row]s for which cross-product is to be taken.

    Returns:
        table_out: Set[Row], cross-product of the tables.
    """
    # prefixing columns with table name
    left = _prefix_columns(left, "left")
    right = _prefix_columns(right, "right")

    table_out = {Row({**row_l, **row_r}) for row_l, row_r in product(left, right)}

    return table_out


def theta_join(left: Set[Row], right: Set[Row], conditions: List[Callable]) -> Set[Row]:
    """
    Joins the table according to conditions.

    Args:
        left: Set[Row].
        right: Set[Row].
        conditions: List[Callable], list of conditions to join on. Each condition
            should be a function mapping a tuple of a row from left and right to a Boolean.
            Example: lambda (x, y): x['id'] == y['employee_id']

    Returns:
        joined_table: Set[Row], theta_join of left and right along the conditions.
    """
    # determining the pair of rows which satisfy the conditions
    joined_table = {
        Row({**_prefix_row(row_l, "left"), **_prefix_row(row_r, "right")})
        for row_l, row_r in product(left, right)
        if all([cond(row_l, row_r) for cond in conditions])
    }

    return joined_table


def natural_join(left: Set[Row], right: Set[Row]) -> Set[Row]:
    """
    Natural join of the left and right tables. It is the same as a theta join with
    the condition that matching columns should be equal.

    Args:
         left: Set[Row].
         right: Set[Row].

    Returns:
        joined_table: Set[Row], natural join of left and right.
    """
    common_cols = _columns_in_table(left).intersection(_columns_in_table(right))
    conditions = [lambda x, y: x[col] == y[col] for col in common_cols]
    joined_table = theta_join(left, right, conditions)
    return joined_table


def _pad_table(table: Set[Row], with_cols: List):
    padding_row = {col: None for col in with_cols}
    padded_table = {Row({**row, **padding_row}) for row in table}
    return padded_table


def union(left: Set[Row], right: Set[Row]) -> Set[Row]:
    """
    Returns the union of the tables.
    Note: this is not the usual set-theoretic union, since duplicates are allowed.

    Args:
        left: Set[Row].
        right: Set[Row].

    Returns:
        table_out: Set[Row], union of the input Set[Row]s.
    """
    # padding
    left_cols = _columns_in_table(left)
    right_cols = _columns_in_table(right)

    left = _pad_table(left, right_cols.difference(left_cols))
    right = _pad_table(right, left_cols.difference(right_cols))

    table_out = left.union(right)

    return table_out


def difference(left: Set[Row], right: Set[Row]) -> Set[Row]:
    """
    Returns the difference of the tables.

    Args:
        left: Set[Row], the table to make difference from.
        right: Set[Row], table to make difference to.

    Returns:
        table_out: Set[Row], union of the input Set[Row]s.
    """
    return left.difference(right)


def intersection(left: Set[Row], right: Set[Row]) -> Set[Row]:
    """
    Returns the intersection of the tables.
    Note: this does not add more expressive power to our already existing operations.
        Intersection can be written as the repeated application of the difference
        operator.

    Args:
        left: Set[Row].
        right: Set[Row].

    Returns:
        table_out: Set[Row], intersection of the input Set[Row]s
    """
    table_out = difference(left, difference(left, right))
    return table_out
