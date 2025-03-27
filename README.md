# About DAG Traverse Celery Example

This project demonstrates the use of Directed Acyclic Graphs (DAGs) for workflow management and task execution using Python. It integrates SQLAlchemy, NetworkX, and Celery to model workflows, manage dependencies, and execute tasks.

## Project Overview

### Code Structure
- `dag/`
  - `__init__.py`: Module initialization
  - `conf.py`: Configuration constants (DATABASE_URI, QUEUE_NAME)
  - `models.py`: SQLAlchemy models (Workflow, Task, Deployment)
  - `run.py`: Database setup and workflow execution
  - `task.py`: Celery task definitions

### Key Features
- **Workflow Management**: DAG-based workflow execution with dependency tracking
- **Multiple Execution Modes**: Single, Mass, Rerun, and Batch processing
- **Priority System**: Regular and Scheduled workflow types
- **Database Integration**: MySQL backend for persistent storage
- **Message Queue**: RabbitMQ for task distribution

## Setup and Installation

### Dependencies
1. **Database (MySQL)**:
```bash
docker run --name dag_celery_db -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root_password \
  -e MYSQL_USER=admin \
  -e MYSQL_PASSWORD=admin123 \
  -e MYSQL_DATABASE=dag_celery mysql:8.0
```

2. **Message Queue (RabbitMQ)**:
```bash
docker run --rm -it -p 15672:15672 -p 5672:5672 rabbitmq:3-management
```

3. **Python Dependencies**:
```bash
pip install -r requirements.txt
```

### Configuration
```bash
# Environment Variables
CELERY_BROKER_URL=amqp://guest:guest@localhost:5672//
DATABASE_URI=mysql+pymysql://admin:admin123@localhost:3306/dag_celery
QUEUE_NAME=celery
```

## Usage

### Basic Execution
```bash
# Start Celery Worker
celery -A dag.task worker --loglevel=INFO

# Run Workflow
python -m dag.run --prio_type [regular|scheduled] --mode [single|mass|rerun-single|all]
```

### Workflow Types and Examples
```
# Food Preparation DAG Examples
Pizza:  base -> sauce -> cheese -> toppings
Pasta:  pasta -> sauce -> cheese -> toppings
Burger: bun -> patty -> cheese -> toppings
Sushi:  rice -> fish -> seaweed -> toppings
```

## Monitoring and Management

### Management UIs
- RabbitMQ: http://localhost:15672 (guest/guest)
- Celery Flower: http://localhost:5555 (when enabled)

### Logging
- Application: logs/app.log
- Celery: logs/celery.log
- Debug Mode: LOG_LEVEL=DEBUG

## Troubleshooting

Common issues and solutions:
1. **Database Connection**: Verify MySQL container and credentials
2. **Message Queue**: Check RabbitMQ status and broker URL
3. **Task Failures**: Review Celery worker logs and dependencies

## Contributing
1. Fork repository
2. Create feature branch
3. Submit pull request with tests and documentation