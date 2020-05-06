from databases.tables import *
from databases.operations import *


employees = {make_employee(0, "Michael Scott", "Regional Manager", 100000),
             make_employee(1, "Dwight K. Schrute", "Assistant to the Regional Manager", 65000),
             make_employee(2, "Pamela Beesly", "Sales", 40000),
             make_employee(3, "James Halpert", "Sales", 55000),
             make_employee(4, "Stanley Hudson", "Sales", 55000)}


tasks = {make_task(0, 0, False),
         make_task(1, 0, False),
         make_task(2, 1, True),
         make_task(3, 1, True),
         make_task(4, 1, True),
         make_task(5, 2, True),
         make_task(6, 3, False),
         make_task(7, 3, False),
         make_task(8, 3, True),
         make_task(9, 3, False),}


clients = {make_client(0, "Dunmore High School", 3),
           make_client(1, "Lackawanna County", 0),
           make_client(2, "Mr. Deckert", 1),
           make_client(3, "Phil Maguire", 3),
           make_client(4, "Harper Collins", 1),
           make_client(5, "Apex Technology"), 1}


project(employees, ["salary"])
select(employees, [lambda x: x["salary"] > 60000])
rename(employees, {"name": "full name"})
cross_product(left=employees, right=tasks)
natural_join(left=employees, right=tasks)
theta_join(left=employees, right=tasks, conditions=[lambda x, y: x["id"] == y["employee_id"]])
union(employees, tasks)
difference(employees, tasks)
intersection(employees, tasks)