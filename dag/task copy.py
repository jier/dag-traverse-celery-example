import time
from celery import Celery, group, chord
from .models import Task, Workflow
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .conf import DATABASE_URI
from celery.result import AsyncResult
from celery.signals import task_postrun, task_prerun, after_setup_logger, task_failure
from celery import states
# from celery import current_task

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
           not AsyncResult(dep_task.celery_task_uid).state == states.SUCCESS:
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



# TODO add data before sending to simulate task in a new deployment task table
@app.task(bind=True)
def _update_deployment_task(self, task_dict):
    pass 

def _has_dependencies(self, task: Task, session) -> bool:
    dependencies = task.dependencies or []
    return any(session.query(Task).filter(Task.id.in_(dependencies))).all()

@app.task(bind=True)
def _process_task(self, task_dict):
    try:
        with session.begin():
            task = Task.from_dict(task_dict)
            task.celery_task_uid = self.request.id
            session.add(task)

            #simulate that task runs
            for i in range(task.sleep):
                print('Type task: {} with Id: {}:  excuting method Sleep, sec: {}'.format(task.type,  task.celery_task_uid, i))
                time.sleep(1)

            self.update_state(state=states.SUCCESS)
            task.celery_task_status = states.SUCCESS
            session.add(task)
            
        print('Task type {} completed with status {}'.format(task.type, task.celery_task_status))
 
        return task.to_dict()
    except Exception as e:
        session.rollback()
        raise e

@app.task(bind=True)
def _update_workflow_status(self,task_list_dict):
    with session.begin():   
        print('Updating Workflow id {}  with children status'.format(task_list_dict[0]['parent_id']))
        
        workflow_id = task_list_dict[0]['parent_id']
        workflow = session.query(Workflow).filter_by(id=workflow_id).one()
        task_list = [Task.from_dict(task_dict) for task_dict in task_list_dict]

        result = []

        for task in task_list:
            result.append({'type': task.type, 'celery_task_uuid': task.celery_task_uid,
                                    'celery_task_status':task.celery_task_status
                                })
            
        workflow.tasks_status = result
                    
        self.update_state(state=states.SUCCESS)
        workflow.status = states.SUCCESS
        session.add(workflow)

        print('Workflow id {} updated with children\'s status'.format(workflow_id))

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

    self.update_state(state=states.SUCCESS)

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
    
    self.update_state(state=states.PENDING)

@app.task(bind=True)
def run_group(self, workflow_id, queue):
    try:
        with session.begin():
            print('Runnning Workflow no graph {} and Task {}'.format(workflow_id, self.request.id))
            workflow = session.query(Workflow).filter_by(id=workflow_id).one()
            
            workflow_tasks = workflow.children
            # Convert task objects to dictionaries
            tasks = [task.to_dict() for task in workflow_tasks]  
            # Update the workflow status and task status
            self.update_state(state=states.PENDING)
            workflow.status = states.PENDING

        # Process each chunk of tasks and update the workflow status as callback
        tasks_chord = group([_process_task.s(task) for task in tasks], link=_update_workflow_status.s( ))
        session.add(workflow)
        # Apply the tasks chord asynchronously to the queue
        tasks_chord.apply_async(queue=queue)


    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.commit()
    
    return True



    
  
