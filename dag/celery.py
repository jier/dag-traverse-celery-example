from celery import Celery
from celery.schedules import crontab
from .conf import DATABASE_URI,QUEUE_NAME_R, QUEUE_NAME_S


# Initialize Celery app
app = Celery('dag-celery',
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
    task_default_priority=5
)
app.conf.update(
    result_expires=3600,
    task_serializer='json',
    accept_content=['json'],  
    result_serializer='json',
    timezone='Europe/Amsterdam',
    enable_utc=True,
)

app.conf.beat_schedule = {
    'run-inpesction-celery-tasks':{
        'task': 'scheduled_monitoring_task',
        'schedule': crontab(minute='*/1'), # run every minute
    }
}
# If this module is run directly
if __name__ == '__main__':
    app.start()