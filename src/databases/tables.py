from typing import Set


class Row(dict):
    def __hash__(self):
        proxy = tuple(self.items())
        return hash(proxy)

    def __setitem__(self, key, value):
        raise NotImplemented("Modifying values is not supported.")


def make_employee(id: int, name: str, position: str, salary: int):
    return Row({"id": id, "name": name, "position": position, "salary": salary})


def make_task(id: int, employee_id: int, completed: bool):
    return Row({"id": id, "employee_id": employee_id, "completed": completed})


def _columns_in_table(table: Set[Row]) -> set:
    return set.union(*[set(record.keys()) for record in table])


def _prefix_row(row: dict, prefix: str) -> dict:
    return Row({f"{prefix}.{key}": value for key, value in row.items()})


def _prefix_columns(table: Set[Row], prefix: str) -> Set[Row]:
    return {_prefix_row(row, prefix) for row in table}
