from typing import List


Table = List[dict]


def make_employee(id: int, name: str, position: str, salary: int):
    return {"id": id, "name": name, "position": position, "salary": salary}


def make_task(id: int, employee_id: int, completed: bool):
    return {"id": id, "employee_id": employee_id, "completed": completed}


def _columns_in_table(table: Table) -> set:
    return set.union(*[set(record.keys()) for record in table])


def _prefix_row(row: dict, prefix: str) -> dict:
    return {f"{prefix}.{key}": value for key, value in row.items()}


def _prefix_columns(table: Table, prefix: str) -> Table:
    return [_prefix_row(row, prefix) for row in table]


employees = [make_employee(0, "Michael Scott", "Regional Manager", 100000),
             make_employee(1, "Dwight K. Schrute", "Assistant to the Regional Manager", 65000),
             make_employee(2, "Pamela Beesly", "Receptionist", 40000),
             make_employee(3, "James Halpert", "Sales", 55000),
             make_employee(4, "Stanley Hudson", "Sales", 60000)]


tasks = [make_task(0, 0, False),
         make_task(1, 0, False),
         make_task(2, 1, True),
         make_task(3, 1, True),
         make_task(4, 1, False),
         make_task(5, 2, True),
         make_task(6, 3, False),
         make_task(7, 3, False),
         make_task(8, 3, True),
         make_task(9, 3, False),]
