import time
from celery import Celery
from .models import Task, Workflow
from .conf import DATABASE_URI
# from celery.signals import task_postrun, task_prerun, after_setup_logger, task_failure
from celery.states import SUCCESS
from celery import group
import networkx as nx
from celery.result import AsyncResult

app = Celery('dag-celery', backend='db+' + DATABASE_URI, broker='amqp://guest:guest@localhost')


def _process_task_node(task_dict, uid):
    task = Task.from_dict(task_dict)
    task.celery_task_uid = uid

    # simulate that task runs
    for i in range(task.sleep):
        print('Simulation run with type: {}  Id: {} with method Sleep, sec: {}'.format(task.type, uid, i))
        time.sleep(1)
    task.celery_task_status = SUCCESS
    print('Task type {} completed with status {}'.format(task.type, task.celery_task_status))

    return task.to_dict()


def flatten_workflow_dag(workflow: Workflow):
    return nx.topological_sort(workflow.execution_graph)


@app.task(bind=True, idempotent=True)
def run(self, workflow_dict):

    workflow = Workflow.from_dict(workflow_dict)
    print('Running Workflow {} '.format(workflow.id))

    flattened_workflow = flatten_workflow_dag(workflow)
    task_list = []

    for task_id in flattened_workflow:
        task_dict = _process_task_node(workflow.get_child(task_id).to_dict(), self.request.id)
        task_list.append(Task.from_dict(task_dict))

    result = []
    for task in task_list:
        result.append({'type': task.type, 'celery_task_uuid': task.celery_task_uid,
                                'celery_task_status':task.celery_task_status
                            })
    workflow.tasks_status = result
    self.update_state(state=SUCCESS, meta={'workflow_id': workflow.id})
    workflow.status = SUCCESS

    return workflow.to_dict()


@app.task(bind=True)
def _process_task(self, task_dict):

    task_dict['celery_task_uid'] = self.request.id
    task = Task.from_dict(task_dict)

    #simulate that task runs
    for i in range(task.sleep):
        print('Type task:{} Id: {}: Sleep, sec: {}'.format(task.type,  task.celery_task_uid, i))
        time.sleep(1)

    self.update_state(state=SUCCESS)
    task.celery_task_status = SUCCESS

    print('Task type {} completed with status {}'.format(task.type, task.celery_task_status))

    return task.to_dict()

# TODO add data before sending to simulate task in a new deployment task table
def _update_deployment_task(self, task_dict):
    pass 



def _update_workflow_status(task_list_dict, workflow_dict):

    print('Updating Workflow id {}  with children status'.format(task_list_dict[0]['parent_id']))
    
    workflow = Workflow.from_dict(workflow_dict)
    task_list = [Task.from_dict(task_dict) for task_dict in task_list_dict]
  
    result = []
    for task in task_list:
        result.append({'type': task.type, 'celery_task_uuid': task.celery_task_uid,
                                'celery_task_status':task.celery_task_status
                            })
    workflow.tasks_status = result

    workflow.status = SUCCESS
    print('Workflow id {} updated with children status'.format(workflow.id))

    return workflow.to_dict()
    

@app.task(bind=True)
def run_group(self, workflow_dict, queue):

    workflow = Workflow.from_dict(workflow_dict)
    print('Runnning Workflow no graph {} and Task {}'.format(workflow.id, self.request.id))

    workflow_tasks = workflow.children

    # Convert task objects to dictionaries
    tasks = [task.to_dict() for task in workflow_tasks]  

    # Process each chunk of tasks and update the workflow status 
    tasks_group = group([_process_task.s(task) for task in tasks]) 

    result_group =tasks_group.apply_async(queue=queue)
    intermediate_tasks_result = []
    # Wait for the result of the group once all tasks are done
    while True:
        if result_group.successful() or result_group.failed():
            intermediate_tasks_result.append([AsyncResult(result_group.children[idx]).result  for idx, _ in enumerate(result_group)])
            break
        time.sleep(1)

    return _update_workflow_status(task_list_dict=intermediate_tasks_result[0],workflow_dict=workflow_dict)
