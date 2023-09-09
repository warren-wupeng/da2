# da2
python DDD and EDA framework

# install
pip install da2

# usage

domain layer
```python
from typing import TypeVar

from pydantic import BaseModel
from da2.entity import Entity
from da2.r


EmployeeId = TypeVar(EmployeeId, bound=str)

class EmployeeDescription(BaseModel):
    name: str
    email: str


class Employee(Entity[EmployeeId, EmployeeDesc]):

    pass

class EmployeeRepo(EntityRepo):
    entityClass = Employee
    descriptionClass = EmployeeDescription



from da2.command import Command

class CreateEmployee(Command):
    employeeDesc: EmployeeDescription
```

application layer

```python
from 

def create_employee()
```



