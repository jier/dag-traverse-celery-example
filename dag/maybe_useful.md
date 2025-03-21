Based on the provided code, it appears that you are trying to implement a workflow management system using Celery and SQLAlchemy. Here's a design for the logic of the first TODO:

**Step 1: Define the workflow types**

* Create a `WorkflowType` model that stores the different types of workflows (e.g. regular, scheduled).
* Define the `wf_priority` list as a class attribute of the `WorkflowType` model.

**Step 2: Design the logic to group workflows and tasks**

* Create a `Workflow` model that stores the workflow details (e.g. priority type, DAG adjacency list).
* Create a `Task` model that stores the task details (e.g. sleep time, type, dependencies).
* Define a many-to-one relationship between `Workflow` and `Task` to represent the workflow-task hierarchy.

**Step 3: Set priority using Celery configuration**

* Create a Celery configuration file (e.g. `celeryconfig.py`) that defines the priority queue settings.
* Use the `CELERY_QUEUES` setting to define the priority queues (e.g. `regular`, `scheduled`).
* Use the `CELERY_DEFAULT_QUEUE` setting to define the default queue for tasks.

**Step 4: Add deployment representation of task and workflow**

* Create a `Deployment` model that stores the deployment details (e.g. revision, task workflow).
* Define a many-to-one relationship between `Workflow` and `Deployment` to represent the workflow-deployment hierarchy.

**Step 5: Design logic to choose which workflow to check depending on tasks total time**

* Create a `TaskStatus` model that stores the task status (e.g. running, completed).
* Create a `WorkflowStatus` model that stores the workflow status (e.g. running, completed).
* Define a many-to-one relationship between `Workflow` and `WorkflowStatus` to represent the workflow-status hierarchy.
* Define a many-to-one relationship between `Task` and `TaskStatus` to represent the task-status hierarchy.

**Step 6: Implement the logic to check the workflow type and choose the corresponding queue**

* Create a Celery task (e.g. `check_workflow_status`) that checks the workflow type and chooses the corresponding queue.
* Use the `CELERY_QUEUES` setting to retrieve the priority queues (e.g. `regular`, `scheduled`).
* Use the `CELERY_DEFAULT_QUEUE` setting to retrieve the default queue for tasks.

Here's some sample code to illustrate the design:
```python
from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Define the workflow types
class WorkflowType(Base):
    __tablename__ = 'workflow_types'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    priority = Column(Integer)

# Define the workflows
class Workflow(Base):
    __tablename__ = 'workflows'
    id = Column(Integer, primary_key=True)
    workflow_type_id = Column(Integer, ForeignKey('workflow_types.id'))
    workflow_type = relationship('WorkflowType')
    dag_adjacency_list = Column(Text)

# Define the tasks
class Task(Base):
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflows.id'))
    workflow = relationship('Workflow')
    sleep_time = Column(Integer)
    type = Column(String)
    dependencies = Column(Text)

# Define the deployments
class Deployment(Base):
    __tablename__ = 'deployments'
    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflows.id'))
    workflow = relationship('Workflow')
    revision = Column(String)

# Create the Celery configuration file
celeryconfig.py
# CELERY_QUEUES = [
#     {'name': 'regular', 'exchange
Here's the full example of the `celeryconfig.py` file:
```python
import os

# Define the Celery configuration
CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'amqp://guest:guest@localhost:5672//')
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_QUEUES = [
    {'name': 'regular', 'exchange': 'regular_exchange', 'routing_key': 'regular'},
    {'name': 'scheduled', 'exchange': 'scheduled_exchange', 'routing_key': 'scheduled'},
]
CELERY_DEFAULT_QUEUE = 'regular'
CELERY_DEFAULT_EXCHANGE = 'regular_exchange'
CELERY_DEFAULT_ROUTING_KEY = 'regular'
CELERY_DEFAULT_DELIVERY_MODE = 2  # persistent delivery mode

# Define the priority queues
CELERY_QUEUES.extend([
    {'name': 'high_priority', 'exchange': 'high_priority_exchange', 'routing_key': 'high_priority'},
    {'name': 'low_priority', 'exchange': 'low_priority_exchange', 'routing_key': 'low_priority'},
])
```
This configuration file defines the following settings:

* `CELERY_BROKER_URL`: the URL of the message broker (e.g. RabbitMQ, Redis).
* `CELERY_RESULT_BACKEND`: the URL of the result backend (e.g. Redis, database).
* `CELERY_ACCEPT_CONTENT`: the content types that Celery accepts (e.g. JSON, pickle).
* `CELERY_TASK_SERIALIZER` and `CELERY_RESULT_SERIALIZER`: the serializers used for tasks and results (e.g. JSON, pickle).
* `CELERY_QUEUES`: a list of queues, each defined by a dictionary with the following keys:
	+ `name`: the name of the queue.
	+ `exchange`: the name of the exchange associated with the queue.
	+ `routing_key`: the routing key for the queue.
* `CELERY_DEFAULT_QUEUE`, `CELERY_DEFAULT_EXCHANGE`, and `CELERY_DEFAULT_ROUTING_KEY`: the default queue, exchange, and routing key used for tasks that don't specify a queue.
* `CELERY_DEFAULT_DELIVERY_MODE`: the delivery mode used for tasks that don't specify a delivery mode (e.g. persistent, non-persistent).

You can customize these settings to fit your specific use case.

-------------------
import random
import time
from celery import Celery
from celery.schedules import crontab
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime

# Define the database engine and session maker
engine = create_engine('sqlite:///example.db')
Session = sessionmaker(bind=engine)
session = Session()

# Define the base model
Base = declarative_base()

# Define the Workflow model
class Workflow(Base):
    __tablename__ = 'workflows'
    id = Column(Integer, primary_key=True)
    prio_type = Column(String)
    dag_adjacency_list = Column(Text)
    time_created = Column(DateTime)

# Define the Task model
class Task(Base):
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflows.id'))
    workflow = relationship('Workflow')
    sleep_time = Column(Integer)
    type = Column(String)

# Create the tables
Base.metadata.create_all(engine)

# Define the Celery configuration
celery = Celery('tasks', broker='amqp://guest:guest@localhost:5672//')
celery.conf.beat_schedule = {
    'run-prio-queue': {
        'task': 'run_task',
        'schedule': crontab(minute='*/1'),  # run every 1 minute
        'args': (1,)  # priority queue
    },
    'run-default-queue': {
        'task': 'run_task',
        'schedule': crontab(minute='*/2'),  # run every 2 minutes
        'args': (0,)  # default queue
    }
}

# Create 10 workflows with random priority type and DAG adjacency list
for i in range(10):
    prio_type = random.choice(['regular', 'scheduled'])
    dag_adjacency_list = [random.choice(['A', 'B', 'C']) for _ in range(random.randint(1, 10))]
    workflow = Workflow(prio_type=prio_type, dag_adjacency_list=str(dag_adjacency_list))
    session.add(workflow)
    session.commit()

# Get the workflows and check their priority type and time created
workflows = session.query(Workflow).all()
for workflow in workflows:
    print(f'Workflow ID: {workflow.id}, Priority Type: {workflow.prio_type}, Time Created: {workflow.time_created}')

# Create target systems with time.sleep commands
target_systems = []
for i in range(4):
    target_system = []
    for j in range(random.randint(4, 10)):
        target_system.append(time.sleep(random.randint(4, 10)))
    target_systems.append(target_system)

# Define the logic to group workflows and tasks and set priority using Celery configuration
def group_workflows(workflows):
    grouped_workflows = {}
    for workflow in workflows:
        if workflow.prio_type not in grouped_workflows:
            grouped_workflows[workflow.prio_type] = []
        grouped_workflows[workflow.prio_type].append(workflow)
    return grouped_workflows

# Define the logic to keep states of target systems
def keep_target_system_states(target_systems):
    target_system_states = {}
    for i, target_system in enumerate(target_systems):
        target_system_states[i] = {'state': 'running', 'tasks': []}
    return target_system_states

# Define the logic to add deployment representation of task and workflow and keep revision
class Deployment(Base):
    __tablename__ = 'deployments'
    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflows.id'))
    workflow = relationship('Workflow')
    revision = Column

-------------------------------------
import random
from datetime import datetime
import time
from celery import current_app

class Workflow:
    def __init__(self):
        self.id = None
        self.prio_type = None
        self.tasks = []
        self.time_created = datetime.now()

class Task:
    def __init__(self, name):
        self.name = name

class TargetSystem:
    def __init__(self):
        self.available_time = 0
        self.tasks_running = []

    def add_task(self, workflow):
        self.tasks_running.append(workflow)
        for task in workflow.tasks:
            time.sleep(random.randint(4, 10))
            print(f"Task {task.name} finished")
            self.available_time += 1

class WorkflowRepository:
    def __init__(self):
        self.workflows = []

    def create_workflows(self, n):
        for i in range(n):
            workflow = Workflow()
            workflow.id = f"W{i}"
            workflow.prio_type = random.choice(["high", "low"])
            num_tasks = random.randint(1, 10)
            for j in range(num_tasks):
                task_name = f"T{j}_{i}"
                task = Task(task_name)
                workflow.tasks.append(task)
            self.workflows.append(workflow)

    def get_workflows(self):
        return self.workflows

class TargetSystemRepository:
    def __init__(self):
        self.target_systems = [TargetSystem() for _ in range(2)]

    def add_workflow(self, workflow):
        for target_system in self.target_systems:
            if len(target_system.tasks_running) < sum(len(tasks) for t in target_system.tasks_running):
                target_system.add_task(workflow)
                print(f"Workflow {workflow.id} assigned to Target System")
                return
        print(f"No available time slots in Target Systems for workflow {workflow.id}")

def design_logic():
    # Design logic to group workflows and tasks and set priority using celery configuration
    pass

def keep_states():
    # Keep states of target systems in order to call celery tasks
    pass

def deployment_representation():
    # Add deployment representation of task and workflow and keep revision
    pass

def get_deployments():
    # Get deployments where status has incomplete, extract their workflow_id's, group them by type and put to scheduler
    pass

# Create workflows
repository = WorkflowRepository()
repository.create_workflows(10)

# Check if workflows are correctly created
for workflow in repository.get_workflows():
    print(f"Workflow {workflow.id} - Prio Type: {workflow.prio_type}, Time Created: {workflow.time_created}")

# Target System Repository
target_system_repository = TargetSystemRepository()

# Loop through workflows and assign them to target systems based on available time
while True:
    for workflow in repository.get_workflows():
        target_system_repository.add_workflow(workflow)
