from .models import Workflow, Task, CeleryTask, Base
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from .conf import DATABASE_URI, QUEUE_NAME, QUEUE_NAME_2
from .task import run_group # run, run_no_graph
import random

engine = create_engine(DATABASE_URI)

Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.drop_all(engine)
try:
    CeleryTask.__table__.drop(engine)
except OperationalError:
    print(f"Table '{CeleryTask.__tablename__}' does not exist. No action taken.")
CeleryTask.__table__.create(engine, checkfirst=True)
Base.metadata.create_all(engine)
# TODO fix indexing of tasks
order_dependency = dict([
    (1, [3]),
    (2, [4]),
    (3, [5]),
    (4, [5]),
    (5, [6, 7]),
    (6, [8]),
    (7, [8])
])

make_dependency = dict([
    (1, [3]),
    (2, [3]),
    (3, [4]),
    (4, [])
])
prio_dependency = dict([])

tastes = ['pizza', 'pasta', 'burger', 'sushi']

pizza_order =['dough', 'sauce', 'cheese', 'toppings']
pasta_order = ['pasta', 'sauce', 'cheese', 'toppings']
burger_order = ['bun', 'patty', 'cheese', 'toppings']
sushi_order = ['rice', 'fish', 'seaweed', 'toppings']

pizzas = ['margherita', 'pepperoni', 'hawaiian', 'meat feast']
pastas = ['carbonara', 'bolognese', 'pesto', 'alfredo']
burgers = ['cheeseburger', 'chicken burger', 'veggie burger', 'bacon burger']
sushis = ['nigiri', 'sashimi', 'maki', 'temaki']

pizza_order_dict = {idx: steps for idx, steps in enumerate(pizza_order)}
pasta_order_dict = {idx: steps for idx, steps in enumerate(pasta_order)}
burger_order_dict = {idx: steps for idx, steps in enumerate(burger_order)}
sushi_order_dict = {idx: steps for idx, steps in enumerate(sushi_order)}
wf_priority = ['regular','scheduled']

# TODO Design logic to group workflows and tasks and set priority 
# TODO find logic to derive dependencies from data 
# TODO express outside type of dependencies pasta depends on pizza depnds on burger

# parent_regular = Workflow(prio_type='regular', dag_adjacency_list=make_dependency)
# session.add(parent_regular)
# for i in range(len(pizza_order)):
#     parent_regular.children.append(Task(sleep=random.randint(1, 4),type=random.choice(tastes), dependencies={}))
# session.commit()

# workflow_regular = session.query(Workflow).filter_by(prio_type='regular').first()
# run.apply_async(
#     args=(workflow_regular.id,QUEUE_NAME,),
#     queue=QUEUE_NAME
# )

parent_scheduled = Workflow(prio_type='scheduled', dag_adjacency_list=[])
session.add(parent_scheduled)
for i in range(len(pasta_order)):
    parent_scheduled.children.append(Task(sleep=random.randint(1, 4),type=random.choice(pastas), dependencies={}))
session.commit()

workflow_scheduled = session.query(Workflow).filter_by(prio_type='scheduled').first()
run_group.apply_async(
    args=(workflow_scheduled.id,QUEUE_NAME_2,),
    queue=QUEUE_NAME_2
)