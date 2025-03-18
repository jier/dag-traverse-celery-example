import time
from celery import Celery, group
from .models import Task, Workflow
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .conf import DATABASE_URI
from celery.result import AsyncResult
from celery.signals import task_postrun, task_prerun, after_setup_logger, task_failure
from celery.states import SUCCESS

app = Celery('dag-celery', backend='db+' + DATABASE_URI, broker='amqp://guest:guest@localhost')

engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)

def find_entry_point(G):
    result = []
    for node in G.nodes:
        if len(list(G.predecessors(node))) == 0:
            result.append(node)
    return result

@task_prerun.connect
def prerun(*args, **kwargs):
    global session
    session = Session()


@task_postrun.connect
def postrun(*args, **kwargs):
    session.flush()
    session.close()

def _is_node_rdy(task, graph):
    tasks = session.query(Task).filter(Task.id.in_(list(graph.predecessors(task.id)))).all()
    for dep_task in tasks:
        if not dep_task.celery_task_uid or \
           not AsyncResult(dep_task.celery_task_uid).state == SUCCESS:
            return False
    return True

def _process_task_node(task, uid):
    task.celery_task_uid = uid
    session.add(task)
    session.commit()

    # simulate that task runs
    for i in range(task.sleep):
        print('Type task:{} Id: {}: Sleep, sec: {}'.format(task.type, uid, i))
        time.sleep(1)

@app.task(bind=True)
def _process_task(self, task_dict):
    task_dict['celery_task_uid'] = self.request.id
    task = Task.from_dict(task_dict)
    session.add(task)
    session.commit()

    #simulate that task runs
    for i in range(task.sleep):
        print('Type task:{} Id: {}: Sleep, sec: {}'.format(task.type,  task.celery_task_uid, i))
        time.sleep(1)
    self.update_state(state=SUCCESS)

def _has_dependencies(self, task: Task, session) -> bool:
    dependencies = task.dependencies or []
    return any(session.query(Task).filter(Task.id.in_(dependencies))).all()

# TODO Update Workflow status once all tasks are completed with task id and status using celery callback
# def _update_workflow_status(workflow_id, task_id, status):
#     workflow = session.query(Workflow).filter_by(id=workflow_id).one()
#     workflow.status = status
#     session.add(workflow)
#     session.commit()  

@app.task(bind=True)
def run(self, workflow_id, queue, cur_task_id=None):
    print('Runnning Workflow {} and Task {}'.format(workflow_id, cur_task_id))
    workflow = session.query(Workflow).filter_by(id=workflow_id).one()
    graph = workflow.execution_graph

    next_task_ids = []
    if cur_task_id:
        task = session.query(Task).get(cur_task_id)
        if not _is_node_rdy(task, graph):
            return

        _process_task_node(task, self.request.id)

        next_task_ids = list(graph.successors(cur_task_id))
    else:
        next_task_ids = find_entry_point(graph)

    self.update_state(state=SUCCESS)

    # Process the next tasks recursively
    for task_id in next_task_ids:
        run.apply_async(
            args=(workflow_id, task_id,),
            queue=queue
        )


@app.task(bind=True)
def run_no_graph(self, workflow_id, queue):
    print('Runnning Workflow no graph {} and Task {}'.format(workflow_id, self.request.id))
    workflow = session.query(Workflow).filter_by(id=workflow_id).one()
    
    workflow_tasks = workflow.children
    # Convert task objects to dictionaries
    tasks = [task.to_dict() for task in workflow_tasks]  

    # Process each chunk of tasks
    tasks_group = group(_process_task.si(task) for task in tasks)
        
    # Apply the tasks group asynchronously to the queue
    tasks_group.apply_async(queue=queue)
    
    self.update_state(state=PROGRESS)

