class Task:
    def __init__(self, name, dependencies=None, prio_type='regular', system_id=None):
        self.name = name
        self.dependencies = dependencies if dependencies else []
        self.prio_type = prio_type
        self.system_id = system_id
        self.feedback_bucket = []
        self.total_status_run = 0

    def add_feedback(self, feedback):
        self.feedback_bucket.append(feedback)

    def increment_status_run(self):
        self.total_status_run += 1


class Workflow:
    def __init__(self):
        self.tasks = []
        self.adjacency_list = {}

    def add_task(self, task):
        self.tasks.append(task)
        if task.dependencies:
            self.adjacency_list[task.name] = task.dependencies
        else:
            self.adjacency_list[task.name] = []

    def get_task(self, name):
        for task in self.tasks:
            if task.name == name:
                return task
        return None

    def run_task(self, name):
        task = self.get_task(name)
        if task:
            task.increment_status_run()
            # Add logic to execute the task
            print(f"Running task: {task.name}")

    def run_all_tasks(self):
        for task in self.tasks:
            self.run_task(task.name)