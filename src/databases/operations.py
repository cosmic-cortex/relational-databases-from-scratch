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
    updated_tables = [rename(table, {column: f"{table_name}.{column}" for column in columns_in_table(table)})
                      for table_name, table in tables.items()]

    # constructing the cross-product
    table_out = [dict(ChainMap(*rows)) for rows in product(*updated_tables)]
    return table_out


def natural_join(**tables) -> Table:
    """
    Creates the natural join of the input tables. The natural join is the cross-product of the tables,
    filtered for records where mutual columns match.

    Args:
        **tables: Tables to be joined

    Returns:
         table_out: Table, natural join of tables
    """

    columns_dict = {
        table_name: columns_in_table(table) for table_name, table in tables.items()
    }
    matching_columns = set.intersection(*list(columns_dict.values()))
    cross_product_table = cross_product(**tables)

    # filtering records where mutual columns are not equal
    table_names = list(tables.keys())
    columns_to_match = [
        (f"{name_1}.{column}", f"{name_2}.{column}")
        for name_1, name_2, column in product(table_names, table_names, matching_columns)
        if name_1 < name_2
    ]
    conditions = [
        lambda record: record[col_1] == record[col_2] for col_1, col_2 in columns_to_match
    ]
    table_out = select(cross_product_table, conditions)

    # removing duplicate columns by projection
    duplicate_columns = {f"{table_name}.{column}": column
                         for table_name, column in product(table_names, matching_columns)}
    table_out = rename(table_out, duplicate_columns)

    return table_out


def union(*tables) -> Table:
    """
    Returns the union of the tables.
    Note: this is not the usual set-theoretic union, since duplicates are allowed.

    Args:
        tables: Tables

    Returns:
        table_out: Table, union of the input Tables
    """

    return reduce(lambda x, y: x + y, tables)


def difference(left: Table, right: Table) -> Table:
    """
    Returns the difference of the tables.

    Args:
        left: Table, the table to make difference from
        right: Table, table to make difference to

    Returns:
        table_out: Table, union of the input Tables
    """
    return [record for record in left if record not in right]


def intersection(left: Table, right: Table) -> Table:
    return difference(left, difference(left, right))
