# DAG Traverse Celery Example

This project demonstrates the use of Directed Acyclic Graphs (DAGs) for workflow management and task execution using Python. It integrates various libraries such as SQLAlchemy, NetworkX, and Celery to model workflows, manage dependencies, and execute tasks.

## Project Structure

- **`dag/test.py`**: Contains the core logic for generating workflows and tasks, including:
  - Definition of `Workflow` and `Task` models using SQLAlchemy.
  - A function to generate adjacency lists for workflows while ensuring they form a valid DAG.
  - Example data for workflows and tasks, including pizza, pasta, burger, and sushi preparation steps.

## Key Features

1. **Workflow and Task Models**:
   - `Workflow`: Represents a workflow with a DAG adjacency list and a method to validate and return the execution graph.
   - `Task`: Represents individual tasks linked to workflows.

2. **Adjacency List Generation**:
   - The `generate_adjacency_list` function creates adjacency lists for workflows and validates them as DAGs using NetworkX.

3. **Example Data**:
   - Predefined workflows for food preparation (e.g., pizza, pasta, burger, sushi) with their respective steps and dependencies.

4. **Database Integration**:
   - SQLAlchemy is used to define models and connect to a MySQL database for storing workflows and tasks.

### About
This project is an example project that complements a blog post about how to implement a job scheduler for traversing DAG of tasks in Python's [Celery](http://celery.readthedocs.io)

### How to run

#### Dependencies
1. `MySQL 5.6`+
2. `RabbitMQ`
3. `Python 3`

#### Steps
1. `git clone git@github.com:jier/dag-traverse-celery-example.git`
2. `cd dag-traverse-celery-example`
3. `pip install -r requirements.txt`
4. `docker run --rm -it -p 15672:15672 -p 5672:5672 rabbitmq:3-management`  in a new termnial
5. `docker run --name dag_celery_db -p 3306:3306 -e MYSQL_ROOT_PASSWORD=admin123 -e MYSQL_USER=admin -e MYSQL_PASSWORD=admin123 -e MYSQL_DATABASE=dag_celery mysql:8.0` in a separate terminal
6. `celery -A dag.task worker -Q queue-1` in a separate terminal
7. `python -m dag.run` in a separate terminal

### Code Structure

- `dag/`
  - `__init__.py`: Initializes the `dag` module.
  - `conf.py`: Contains configuration constants like `DATABASE_URI` and `QUEUE_NAME`.
  - `models.py`: Defines the SQLAlchemy models `Workflow`, `Task`, `Deployment`.
  - `run.py`: Sets up the database, creates tasks and workflows, and starts the Celery task.
  - `task.py`: Defines the Celery tasks and their execution logic.

## How to Run

1. **Set Up the Database**:
   - Use the following Docker command to start a MySQL database in a new terminal:
     ```bash
     docker run --name dag_celery_db -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root_password -e MYSQL_USER=admin -e MYSQL_PASSWORD=admin123 -e MYSQL_DATABASE=dag_celery mysql:8.0
     ```

2. **Set Up RabbitMQ**:
    - Use the following Docker command to start a RabbitMQ in a new terminal:
     ```bash
      docker run --rm -it -p 15672:15672 -p 5672:5672 rabbitmq:3-management
     ```

3. **Install Dependencies**:
   - Install the required Python libraries:
     ```bash
     pip install -r requirements.txt
     ```

4. **Run the Script**:
   - Execute the `test.py` script to generate workflows, validate DAGs, and print example outputs:
     ```bash
     python -m dag.run --prio_type ['regular', 'scheduled'] --mode ['single', 'mass', 'rerun-single', 'all']
     ```

## Example Outputs

- **Task Entries**:
  Randomly generated task entries with dependencies.
- **Workflow Entries**:
  Workflow priorities and their respective adjacency lists.
- **Adjacency Lists**:
  Validated adjacency lists for workflows like pizza preparation.

## Notes

- Ensure the database is running before executing the script.
- Modify the `DATABASE_URI` in `test.py` if using a different database configuration.