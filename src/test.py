from databases.tables import *
from databases.operations import *

project(employees, ["salary"])
select(employees, [lambda x: x["salary"] > 60000])
rename(employees, {"name": "full name"})
cross_product(left=employees, right=tasks)
natural_join(left=employees, right=tasks)
theta_join(left=employees, right=tasks, conditions=[lambda x, y: x["id"] == y["employee_id"]])
union(employees, tasks)
difference(employees, tasks)
intersection(employees, tasks)