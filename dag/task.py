import time
from .models import Task, Workflow,Deployment
from .conf import QUEUE_NAME_S, QUEUE_NAME_R
from .celery import app
from celery.states import SUCCESS
from celery import group
import networkx as nx
from celery.result import AsyncResult
import uuid
from collections import deque


def _update_deployment(workflow_dict, task_list_dict, request_id):
    print('Updating deployment with corresponding Workflow id {}'.format(workflow_dict['id']))
    workflow = Workflow.from_dict(workflow_dict)
    task_list = [Task.from_dict(task_dict) for task_dict in task_list_dict]
  
    result = []
    for task in task_list:
        result.append({'type': task.type, 'celery_task_uuid': task.celery_task_uid,
                                'celery_task_status':task.celery_task_status
                            })
    deployment = Deployment(workflow_id=workflow.id)
    deployment.run_id =  request_id
    deployment.revision = str(uuid.uuid4())
    deployment.status = SUCCESS
    deployment.deployment_type = workflow.prio_type
    deployment.deployment_task_status = result
    workflow.deployments.append(deployment)
    return (workflow.to_dict(),deployment.to_dict())


def _update_workflow_status(task_list_dict, workflow_dict):
    # TODO add randomness to sometimes put workflow to failed states depending on task_children status, 
    # all success wf success, at least one failed wf status failed

    print('Updating Workflow id {}  with children status'.format(task_list_dict[0]['parent_id']))
    
    workflow = Workflow.from_dict(workflow_dict)
    task_list = [Task.from_dict(task_dict) for task_dict in task_list_dict]
  
    result = []
    for task in task_list:
        print('Task id before updating: {}'.format(task.id))
        result.append({'type': task.type, 'celery_task_uuid': task.celery_task_uid,
                                'celery_task_status':task.celery_task_status
                            })
    workflow.tasks_status = result
    workflow.children = []
    workflow.children = task_list
    print('Children updated with task id and parent id  {}'.format([(task.to_dict()['id'],task.to_dict()['parent_id']) for task in workflow.children ]))
    workflow.status = SUCCESS
    print('Workflow id {} updated with children status'.format(workflow.id))

    return workflow.to_dict()

@app.task(bind=True)
def _process_task_node(self, task_dict):
    # TODO add randomness to sometimes put task to failed states
    task = Task.from_dict(task_dict)
    task.celery_task_uid = self.request.id

    # simulate that task runs
    for i in range(task.sleep):
        print('Simulation run with type: {}  Task Id: {} uuid: '
        '{} with method Sleep, sec: {}'.format(task.type, task.id, self.request.id, i))
        time.sleep(1)
    task.celery_task_status = SUCCESS
    
    print('Task type {} and Id: '
    '{} completed with status {}'.format(task.type, task.id, task.celery_task_status))
    self.update_state(state=SUCCESS, meta={'tasks_id': task.id})
    return task.to_dict()


def flatten_workflow_dag(workflow: Workflow):
    return nx.topological_sort(workflow.execution_graph)

@app.task(bind=True, exchange=QUEUE_NAME_R)
def run_with_queue_order(self, workflow_dict, queue_):
    workflow = Workflow.from_dict(workflow_dict)
    print('Running Workflow {} '.format(workflow.id))
    graph = workflow.execution_graph
    no_predecessors = [node for node in graph.nodes()
                    if len(list(graph.predecessors(node)))==0]
    print("Nodes with no predecessors:{}".format(no_predecessors))

    visited_node_set = set()
    queue = deque(no_predecessors)
    task_list_dict_async = []
    while queue:
        node = queue.popleft()
        print(f"Starting node: {node}")
        if node not in visited_node_set:
            print(f"Processing node {node}")
            visited_node_set.add(node)
            process_result = _process_task_node.apply_async(
                args=(workflow.get_child(node).to_dict(),),
                queue=queue_)
            task_list_dict_async.append(process_result)
            for neighbor in graph.neighbors(node):
                print(f'Look for neighbours for node {node}')
                if neighbor not in visited_node_set:
                    if not graph.in_degree(neighbor) == 0:
                        print('No predecessor in neighbour, append to process neighbour: {}'.format(neighbor))
                        queue.append(neighbor)
    task_list_dict = []
    while len(task_list_dict_async) > 0:
        for async_elem in task_list_dict_async[:]:
            if async_elem.successful() or async_elem.failed():
                task_list_dict.append(AsyncResult(async_elem).result)
                task_list_dict_async.remove(async_elem)
        print("⏳Waiting for async results to be collected in regular call.")
        time.sleep(1)

    _workflow_dict = _update_workflow_status(task_list_dict=task_list_dict, workflow_dict=workflow.to_dict())
    updated_workflow_dict, updated_deployment_dict = _update_deployment(workflow_dict=_workflow_dict, 
                                                                        task_list_dict=task_list_dict, request_id=self.request.id)
    self.update_state(state=SUCCESS, meta={'workflow_id': workflow.id})
    return dict({'workflow_dict':updated_workflow_dict, 
            'deployment_dict':updated_deployment_dict})

@app.task(bind=True, exchange=QUEUE_NAME_R)
def run_on_topological_sort_graph(self, workflow_dict):
    # Below runs concurrent and does not guarantee the order of the adjacency list of the graph
    workflow = Workflow.from_dict(workflow_dict)
    print('Running Workflow {} '.format(workflow.id))

    flattened_workflow = flatten_workflow_dag(workflow)
    task_list_dict_async = []

    for task_id in flattened_workflow:
        print(f'Task id: {task_id}')
        task_dict = _process_task_node.apply_async(
            args=(workflow.get_child(task_id).to_dict(),),
            queue=QUEUE_NAME_R
        )
        task_list_dict_async.append(task_dict)
    task_list_dict = []
    while len(task_list_dict_async) > 0:
        for async_elem in task_list_dict_async[:]:
            if async_elem.successful() or async_elem.failed():
                task_list_dict.append(AsyncResult(async_elem).result)
                task_list_dict_async.remove(async_elem)
        time.sleep(1)


    
    _workflow_dict = _update_workflow_status(task_list_dict=task_list_dict, workflow_dict=workflow.to_dict())
    updated_workflow_dict, updated_deployment_dict = _update_deployment(workflow_dict=_workflow_dict, 
                                                                        task_list_dict=task_list_dict, request_id=self.request.id)
    self.update_state(state=SUCCESS, meta={'workflow_id': workflow.id})
    return dict({'workflow_dict':updated_workflow_dict, 
            'deployment_dict':updated_deployment_dict})


@app.task(bind=True)
def _process_task(self, task_dict):
    # TODO add randomness to sometimes put task to failed states

    task_dict['celery_task_uid'] = self.request.id
    task = Task.from_dict(task_dict)

    #simulate that task runs
    for i in range(task.sleep):
        print('Type task:{} Id: {}: Sleep, sec: {}'.format(task.type,  task.celery_task_uid, i))
        time.sleep(1)

    self.update_state(state=SUCCESS, meta={'tasks_id': task.id})
    task.celery_task_status = SUCCESS

    print('Task type {} completed with status {}'.format(task.type, task.celery_task_status))

    return task.to_dict()



@app.task(bind=True, exchange=QUEUE_NAME_S)
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
        print("⏳Waiting for async results to be collected in scheduled call.")
        time.sleep(1)
    # intermediate_tasks_result[0] because we want only the results out of the Asyncrecult tuples
    _workflow_dict = _update_workflow_status(task_list_dict=intermediate_tasks_result[0],workflow_dict=workflow_dict)
    updated_workflow_dict, updated_deployment_dict = _update_deployment(workflow_dict=_workflow_dict, task_list_dict=intermediate_tasks_result[0], request_id=self.request.id)
    self.update_state(state=SUCCESS, meta={'workflow_id': workflow.id})

    return dict({'workflow_dict':updated_workflow_dict, 
            'deployment_dict':updated_deployment_dict})


@app.task(name="monitor", bind=True, exchange=QUEUE_NAME_R)
def monitor_workflow_tasks(self):
    """Monitor the progress of workflow tasks periodically via Celery beat"""
    
    # Get all running tasks
    inspector = app.control.inspect()
    active_tasks = inspector.active() or {}
    reserved_tasks = inspector.reserved() or {}
    
    all_tasks = {}
    
    # Combine active and reserved tasks
    for worker, tasks in active_tasks.items():
        for task in tasks:
            all_tasks[task['id']] = task
            
    for worker, tasks in reserved_tasks.items():
        for task in tasks:
            all_tasks[task['id']] = task
            
    # Monitor specific workflow task types
    workflow_tasks = {
        task_id: task for task_id, task in all_tasks.items() 
        if task['name'] in ['task.run_group', 'task.run', 'task.run_with_queue_order']
    }
    
    status_summary = {
        'pending': 0,
        'success': 0,
        'failure': 0,
        'in_progress': 0
    }
    
    # Check status of each workflow task
    for task_id, task in workflow_tasks.items():
        result = AsyncResult(task_id)
        if result.ready():
            if result.successful():
                status_summary['success'] += 1
            else:
                status_summary['failure'] += 1
        elif result.state == 'PENDING':
            status_summary['pending'] += 1
        else:
            status_summary['in_progress'] += 1
    
    print(f"Workflow Tasks Status Summary: {status_summary}")
    return status_summary

@app.task(name="scheduled_monitoring_task", bind=True, exchange=QUEUE_NAME_R)
def call_monitor_workflow_tasks(self):
    monitor_workflow_tasks.apply_async()