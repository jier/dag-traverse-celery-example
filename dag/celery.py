from celery import Celery
from celery.schedules import crontab
from .conf import DATABASE_URI,QUEUE_NAME_R, QUEUE_NAME_S,HEALTH_CELERY_TASK_CRON


# Initialize Celery app
app = Celery('dag_celery',
             broker='amqp://guest:guest@localhost',
             backend='db+' + DATABASE_URI,
             include=['dag.task'])

# Celery configuration
app.conf.update(
    # Define task queues
    task_queues={
        QUEUE_NAME_R: {
            'exchange': QUEUE_NAME_R,
            'exchange_type': 'direct',
            'routing_key': QUEUE_NAME_R,
            'queue_arguments': {'x-max-priority': 10}  # Enable priority with 0-10 levels
        },
        QUEUE_NAME_S: {
            'exchange': QUEUE_NAME_S,
            'exchange_type': 'direct',
            'routing_key': QUEUE_NAME_S
        }
    },
    # Default queue settings
    task_default_queue=QUEUE_NAME_R,
    task_default_exchange=QUEUE_NAME_R,
    task_default_routing_key=QUEUE_NAME_R,
    
    # Enable task priority
    task_inherit_parent_priority=True,
    task_queue_max_priority=10,
    task_default_priority=5,

    #Fine grain control on worker for instance of worker receiving SIGKILL or an exception occured
    # to pass to another worker after it has been put back in the queue
    #  Accepted answer: https://stackoverflow.com/questions/45045980/what-different-between-task-reject-on-worker-lost-and-task-acks-late-in-celery
    task_ack_late=True,
    task_reject_on_worker_lost=True,
    worker_state_db="./celery-state.db" #"./celery-state.db" #only local files works for now
    # result_backend="mysql+pymysql://admin:admin123@dag_celery_db:3306/dag_celery" #not working module not found error
)
app.conf.update(
    result_expires=3600,
    task_serializer='json',
    accept_content=['json'],  
    result_serializer='json',
    timezone='Europe/Amsterdam',
    enable_utc=True,
)

# Health checks summary status of tasks
app.conf.beat_schedule = {
    'run-inpesction-celery-tasks':{
        'task': 'scheduled_monitoring_task',
        'schedule': crontab(minute=HEALTH_CELERY_TASK_CRON), # run every minute
    }
}
# If this module is run directly
if __name__ == '__main__':
    app.start()