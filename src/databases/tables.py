from typing import Set


class Record(dict):
    def __hash__(self):
        proxy = tuple(self.items())
        return hash(proxy)

    def __setitem__(self, key, value):
        raise NotImplemented("Modifying values is not supported.")


def make_employee(id: int, name: str, position: str, salary: int):
    return Record({"id": id, "name": name, "position": position, "salary": salary})


def make_task(id: int, employee_id: int, completed: bool):
    return Record({"id": id, "employee_id": employee_id, "completed": completed})


def _columns_in_table(table: Set[Record]) -> set:
    return set.union(*[set(record.keys()) for record in table])


def _prefix_record(row: dict, prefix: str) -> Record:
    return Record({f"{prefix}.{key}": value for key, value in row.items()})


def _prefix_columns(table: Set[Record], prefix: str) -> Set[Record]:
    return {_prefix_record(row, prefix) for row in table}


def _pad_table(table: Set[Record], with_cols: List):
    padding_row = {col: None for col in with_cols}
    padded_table = {Record({**row, **padding_row}) for row in table}
    return padded_table
