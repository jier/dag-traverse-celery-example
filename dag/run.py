from .models import Workflow, Task, Deployment, CeleryTask, Base
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from .conf import DATABASE_URI, QUEUE_NAME, QUEUE_NAME_2
from .task import  run_group, run
import random
from celery.result import ResultBase
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


order_dependency = dict([
    (1, [3]),
    (2, [4]),
    (3, [5]),
    (4, [5]),
    (5, [6, 7]),
    (6, [8]),
    (7, [8])
])

make_dependency_1 =dict([
    (1, [3]),
    (2, [3]),
    (3, [4, 5]),
    (4, []),
    (5, [])
])
make_dependency_2 =dict([
    (1, [2]),
    (2, [3, 4]),
    (3, [5]),
    (4, [5]),
    (5, [])
])
make_dependency_3 =dict([
    (1, [2]),
    (2, [3]),
    (3, [5]),
    (4, [5]),
    (5, [])
])

variations_depency_list = [make_dependency_1,make_dependency_2,make_dependency_3, order_dependency]


tastes = ['pizza', 'pasta', 'burger', 'sushi']

# type of tasks order in a task
# sauces/patty/fish only when dough/pasta/bun/rice happens together with cheese/seaweed then toppings then serving

pizza_order =['dough', 'cheese','sauce', 'toppings','serving']
pasta_order = ['pasta', 'cheese', 'sauce', 'toppings','serving']
burger_order = ['bun', 'cheese','patty', 'toppings','serving']
sushi_order = ['rice', 'seaweed','fish', 'toppings','serving']

representations =[{'pizza':pizza_order},
                  {'pasta': pasta_order},
                  {'burger': burger_order},
                  {'sushi':sushi_order}]
# type of tasks
pizzas = ['margherita', 'pepperoni', 'hawaiian', 'meat feast']
pastas = ['carbonara', 'bolognese', 'pesto', 'alfredo']
burgers = ['cheeseburger', 'chicken burger', 'veggie burger', 'bacon burger']
sushis = ['nigiri', 'sashimi', 'maki', 'temaki']
grouped_tasks = [{'pizzas':pizzas},{'pastas':pastas},{'burgers':burgers},{'sushis':sushis}]

# task course priority
# main_course requires either drink or appetizer, then it is either dessert or cheese platter
full_course = ['drink','appetizer', 'main_course', 'dessert','cheese platter', 'coffee']

appetizer = ['soup', 'salad', 'bread', 'cheese']
main_course = ['pasta', 'pizza', 'burger', 'sushi']
dessert = ['cake', 'ice_cream', 'pudding', 'fruit']
drink = ['water', 'juice', 'soda', 'wine']

#Within a task 
# use function by looping through like below and use modulo to create adjacency list depending on even/uneven logic
# see example logic above
pizza_order_dict = {idx: steps for idx, steps in enumerate(pizza_order)}
pasta_order_dict = {idx: steps for idx, steps in enumerate(pasta_order)}
burger_order_dict = {idx: steps for idx, steps in enumerate(burger_order)}
sushi_order_dict = {idx: steps for idx, steps in enumerate(sushi_order)}




task_course_priority_tasks = dict([
    ('appetizer', ['soup', 'salad', 'bread', 'cheese']),
    ('main_course', ['pasta', 'pizza', 'burger', 'sushi']),
    ('dessert', ['cake', 'ice_cream', 'pudding', 'fruit']),
    ('drink', ['water', 'juice', 'soda', 'wine'])
])
task_course_priority = dict([
    ('appetizer', ['dessert']),
    ('main_course', ['dessert']),
    ('dessert', ['drink']),
    ('drink', [])
])
task_priority_course = dict([
    (1, [2]),
    (2, [3, 4]),
    (3, []),
    (4, [])
])

wf_priority = ['regular','scheduled']

# Make 10 workflows and randomly choose the prio type and depending on the prio type randomly choose the dag_adjacency_list
# In a loop get the workflows and check their prio_type and time_created
#  with both combinations either put it in the prio queue or default queue but only if you know that target system
# target system for now will be the sum of all tasks in the workflow must not exceed the threshold of available time target_systems (2) can handle
#  target system is for now a list of time.sleep commands between 4-10 seconds and we have four target systems running at all time our webhook will be 
#  to announce once one of the elements is finished to the loop. because if  one becomes available then we can call apply either for run task (prio) or run group task using celery beat
# TODO Design logic to group workflows and tasks and set priority using celery configuration and to keep states of target systems in order to call celery tasks
# TODO add deployment representation of task and workflow and keep revision 
# TODO add logic of getting deployments where status has incomplete, extract their workflow_id's, group them by type and put to scheduler

parent_regular = Workflow(prio_type='regular', dag_adjacency_list=make_dependency_1)
session.add(parent_regular)
for i in range(len(full_course)):
    parent_regular.children.append(Task(sleep=random.randint(1, 4),type=random.choice(full_course), dependencies={}))


parent_scheduled = Workflow(prio_type='scheduled', dag_adjacency_list=[])
session.add(parent_scheduled)
for i in range(len(pasta_order)):
    parent_scheduled.children.append(Task(sleep=random.randint(1, 4),type=random.choice(pastas), dependencies={}))


workflow_regular = session.query(Workflow).filter_by(prio_type='regular').first()
workflow_scheduled = session.query(Workflow).filter_by(prio_type='scheduled').first()



# for worklflow in [workflow_regular, workflow_scheduled]:
#     if worklflow.prio_type != 'scheduled':
#         print(worklflow.prio_type)
#     else:
#         print('different')
# result_group =run_group.apply_async(
#     args=(workflow_scheduled.to_dict(),QUEUE_NAME_2,),
#     queue=QUEUE_NAME_2
# )
result_regular = run.apply_async(
    args=(workflow_regular.to_dict(),),
    queue=QUEUE_NAME
)

# Collect results
# Save result in DB
_, answer = list(result_regular.collect())[0]
# print(answer)
finished_workflow = Workflow.from_dict(answer['workflow_dict'])
finished_deployment = Deployment.from_dict(answer['deployment_dict'])
session.add(finished_deployment)
print('--------------------\n')
print(finished_workflow.to_dict())
print('--------------------\n')
print(finished_deployment.to_dict())
session.commit()
# _, answer =list(result_group.collect())[0]
# print(answer)

# print([result for result in result_group.collect() if not isinstance(result, (ResultBase, tuple))])