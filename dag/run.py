from .models import Workflow, Task, Deployment, CeleryTask, Base
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from .conf import DATABASE_URI, QUEUE_NAME_S, QUEUE_NAME_R
from .task import  run_group, run_with_queue_order #,run
import random
from collections import deque
import uuid
import argparse
engine = create_engine(DATABASE_URI)

Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.drop_all(engine)
try:
    CeleryTask.__table__.drop(engine)
except OperationalError:
    print(f"‼️Table '{CeleryTask.__tablename__}' does not exist. Caught an exception for only the first time of dropping.")
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
    (5, [6]),
    (6, [])
])
make_dependency_2 =dict([
    (1, [2]),
    (2, [3, 4]),
    (3, [5]),
    (4, [5]),
    (5, [6]),
    (6, [])
])
make_dependency_3 =dict([
    (1, [2]),
    (2, [3]),
    (3, [5]),
    (4, [5]),
    (5, []),
    (6, [])
])

variations_dependency_list = [make_dependency_1,make_dependency_2,make_dependency_3]


tastes = ['pizza', 'pasta', 'burger', 'sushi']

# type of tasks order in a task
# sauces/patty/fish only when dough/pasta/bun/rice happens together with cheese/seaweed then toppings then serving

pizza_order = ['dough', 'cheese', 'sauce', 'toppings', 'baking', 'serving']
pasta_order = ['pasta', 'cheese', 'sauce', 'toppings','mixing', 'serving']
burger_order = ['bun', 'cheese','patty', 'grilling', 'toppings','serving']
sushi_order = ['rice', 'seaweed','fish', 'toppings','rolling','serving']

representations =[{'pizza':pizza_order},
                  {'pasta': pasta_order},
                  {'burger': burger_order},
                  {'sushi':sushi_order}]
# type of tasks
pizzas = ['margherita', 'pepperoni', 'hawaiian', 'meat feast']
pastas = ['carbonara', 'bolognese', 'pesto', 'alfredo']
burgers = ['cheeseburger', 'chicken burger', 'veggie burger', 'bacon burger']
sushis = ['nigiri', 'sashimi', 'maki', 'temaki']
grouped_tasks = [pizzas,pastas,burgers,sushis]

# task course priority
# main_course requires either drink or appetizer, then it is either dessert or cheese platter
full_course = ['drink','appetizer', 'main_course', 'dessert','cheese_platter', 'coffee']

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


def fill_database(request_size:int,variations_dependency_list:list, full_course:list, grouped_tasks:list):
    print("🗂️ Start populating Database")
    # seed = 1234
    # random.seed(seed)
    prio_types = ['regular','scheduled']

    for _ in range(request_size):
        # Create workflow with random priority type and dependency list
        prio_type = random.choice(prio_types)
        dag_list = random.choice(variations_dependency_list)
        
        # Create new workflow
        workflow = None
        if prio_type == 'regular':
            workflow = Workflow(prio_type=prio_type, dag_adjacency_list=dag_list)
            # Add random tasks based on full course menu
            for _ in range(len(full_course)):
                workflow.children.append(
                    Task(
                        sleep=random.randint(1, 4),
                        type=f'{random.choice(full_course)}-{str(uuid.uuid4())}',
                        dependencies={}
                    )
                )
        elif prio_type ==  'scheduled':
            workflow = Workflow(prio_type=prio_type, dag_adjacency_list={})
            # Add random tasks based on grouped tasks
            for g in range(len(grouped_tasks)):
                workflow.children.append(
                    Task(
                        sleep=random.randint(1, 4),
                        type=f'{random.choice(grouped_tasks[g])}-{str(uuid.uuid4())}',
                        dependencies={}
                    )
                )
        session.add(workflow)
    session.commit()

    
# Single simulation
def single_simulation(session:sessionmaker, Workflow:Workflow, prio_type:str ='regular')->deque:
    print("⚡️Start Single Simulation.")
    workflow = None
    result = None
    to_run_later_queue = deque([])
    print(f"🕹️ Start the engine! with prio_type: {prio_type}")
    if prio_type == 'scheduled':
        workflow = session.query(Workflow).filter_by(prio_type=prio_type).first()
        result = run_group.apply_async(
                        args=(workflow.to_dict(), QUEUE_NAME_S),
                        queue=QUEUE_NAME_S
                    )
    else:
        workflow = session.query(Workflow).filter_by(prio_type=prio_type).first()
        result = run_with_queue_order.apply_async(
                        args=(workflow.to_dict(), QUEUE_NAME_R),
                        queue=QUEUE_NAME_R
                    )

    try:
        print("🎉Collecting results.")
        _, answer = list(result.collect())[0]
        print("💫Collected results.")
        finished_workflow = Workflow.from_dict(answer['workflow_dict'])
        finished_deployment = Deployment.from_dict(answer['deployment_dict'])
        session.add(finished_deployment)
        session.merge(finished_workflow)
        session.commit()
        print("🎊Result saved to the database.")
    except Exception as err:
        print(f"🧮An error occurred: {err}")
        print("💢 Some tasks failed, appending task to the queue")
        to_run_later_queue.append(workflow)
    
    return to_run_later_queue


# Mass simulation
def mass_simulation(session:sessionmaker, Workflow:Workflow, batch_size:int, available_nodes:int)->deque:
    print("⚡️Start Mass Simulation.")
    workflows = session.query(Workflow).all()
    to_run_later_queue = deque([])
    for i in range(0, len(workflows), batch_size):
        batch = workflows[i:i + batch_size]

        for workflow in batch:
            # Keep track of task execution time and target system availability
            target_systems = [random.randint(50, 100) for _ in range(available_nodes)]
            task_execution_time = sum(task.sleep for task in workflow.children)
            print(f"⏲️All task execution time is of: {task_execution_time}")
            available_target_system = min(target_systems)
            result = None
            if workflow.prio_type == 'scheduled': # prio (Low)
                # Only process if target system can handle the total execution time
                if task_execution_time <= available_target_system: 
                    # Update target system capacity
                    target_systems[target_systems.index(available_target_system)] -= task_execution_time
                    print("✅ Enough Capacity to run scheduled run")
                    result = run_group.apply_async(
                        args=(workflow.to_dict(), QUEUE_NAME_S),
                        queue=QUEUE_NAME_S
                    )
                else:
                    print("❌No capacity on target system for now run will be run later for scheduled.")
                    print("Appending to a queue to be try on later")
                    to_run_later_queue.append(workflow)
            elif workflow.prio_type == 'regular':
                if task_execution_time <= available_target_system:  
                    # regular priority (High)
                    # Update target system capacity
                    print("✅ Enough Capacity to run High priority run")
                    target_systems[target_systems.index(available_target_system)] -= task_execution_time
                    result = run_with_queue_order.apply_async(
                        args=(workflow.to_dict(), QUEUE_NAME_R),
                        queue=QUEUE_NAME_R
                    )
                else:
                    print("❌No capacity on target system for now run will be run later for scheduled.")
                    print("Appending to a queue to be try on later.")
                    to_run_later_queue.append(workflow)
            try:
                # Wait for result
                if result is not None:
                    print("🎉Collecting results")
                    _, answer = list(result.collect())[0]
                    print("💫Collected results.")
                    finished_workflow = Workflow.from_dict(answer['workflow_dict'])
                    finished_deployment = Deployment.from_dict(answer['deployment_dict'])
                    session.add(finished_deployment)
                    session.merge(finished_workflow)
                    session.commit()
                    print("🎊Result saved to the database.")
            except Exception as err:
                print(f"🧮An error occurred: {err}")
                print("💢 Some tasks failed")
                # Maybe add to the queue to run  later?
                to_run_later_queue.append(workflow)
    
    print("⚡️End of Simulation.")

    return to_run_later_queue 

# Rerun failed simulation        
def rerun_failed_not_completed_workflow(to_run_later_queue:deque):
    
    print("💥Start Rerunning incomplete or failed workflows.")
    counter_refusal_regular = 0
    counter_refusal_scheduled = 0

    while to_run_later_queue:
        workflow = to_run_later_queue.popleft()
        # Keep track of task execution time and target system availability
        task_execution_time = sum(task.sleep for task in workflow.children)
        target_systems = [random.randint(50, 100) for _ in range(4)]
        print(f"⏲️All task execution time is of: {task_execution_time}")
        available_target_system = min(target_systems)
        result = None
        if workflow.prio_type == 'regular':
            if task_execution_time <= available_target_system:
                print("✅ Enough Capacity to run High priority run in later statge.")
                target_systems[target_systems.index(available_target_system)] -= task_execution_time
                result = run_with_queue_order.apply_async(
                        args=(workflow.to_dict(), QUEUE_NAME_R),
                        queue=QUEUE_NAME_R
                    )
            else:
                print("‼️System not available again for High Prio. We are sorry.")
                counter_refusal_regular +=1
        elif workflow.prio_type == 'scheduled':
            if task_execution_time <= available_target_system:
                print("✅ Enough Capacity to run Low priority run in a later stage.")
                target_systems[target_systems.index(available_target_system)] -= task_execution_time
                result = run_group.apply_async(
                        args=(workflow.to_dict(), QUEUE_NAME_S),
                        queue=QUEUE_NAME_S
                    )
            else:
                print("‼️System not available again for Low Prio. We are sorry.")
                counter_refusal_scheduled +=1
        if counter_refusal_scheduled + counter_refusal_regular > 5:
            print("💣 System overloaded. We are fixing the issue.")
            break

        try:
            # Wait for result
            if result is not None:
                print("🎉Collecting results")
                _, answer = list(result.collect())[0]
                print("💫Collected results.")
                finished_workflow = Workflow.from_dict(answer['workflow_dict'])
                finished_deployment = Deployment.from_dict(answer['deployment_dict'])
                session.add(finished_deployment)
                session.merge(finished_workflow)
                session.commit()
                print("🎊Result saved to the database.")
        except Exception as err:
            print(f"🧮An error occurred: {err}")
            print("💢 Some tasks failed, Maybe fix those??")
            # Maybe add to the queue to run  later?
    
    print("💥Finished Rerunning incomplete or failed workflows.")

        

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='DAG Task Runner')
    parser.add_argument('--mode', type=str, required=True, 
                      choices=['single', 'mass', 'rerun-single', 'all'],
                      help='Mode of operation: single, mass, rerun-single, or all')
    parser.add_argument('--prio_type', type=str, required=True,default='regular',
                        choices=['regular','scheduled'],
                      help='Priority type for simulation requests, '
                      'where regular stands for users request with high priority and '
                      'cheduled request are per type scheduled requests (default: regular), '
                      'options: regular or scheduled')
    parser.add_argument('--batch-size', type=int, default=4,
                      help='Batch size for mass simulation (default: 4)')
    parser.add_argument('--db_size', type=int, default=4,
                      help='Database size for simulation (default: 4)')
    parser.add_argument('--nodes', type=int, default=4, 
                      help='Number of available nodes on target system (default: 4)')
    
    args = parser.parse_args()
    fill_database(request_size=args.db_size, variations_dependency_list=variations_dependency_list,full_course=full_course,grouped_tasks=grouped_tasks)
    if args.mode == 'single':
        failed_queue = single_simulation(session, Workflow, args.prio_type)
        print(f"Number of failed/incomplete workflows: {len(failed_queue)}")
    elif args.mode == 'mass':
        failed_queue = mass_simulation(session, Workflow, args.batch_size, args.nodes)
        print(f"Number of failed/incomplete workflows: {len(failed_queue)}")
    elif args.mode == 'rerun-single':
        # Get failed workflows from previous run
        failed_queue = single_simulation(session, Workflow, args.prio_type)
        if failed_queue:
            rerun_failed_not_completed_workflow(failed_queue)
    elif args.mode == 'all':
        failed_queue = mass_simulation(session, Workflow, args.batch_size, args.nodes)
        if failed_queue:
            rerun_failed_not_completed_workflow(failed_queue)


