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
  - `models.py`: Defines the SQLAlchemy models `Workflow` and `Task`.
  - `run.py`: Sets up the database, creates tasks and workflows, and starts the Celery task.
  - `task.py`: Defines the Celery tasks and their execution logic.