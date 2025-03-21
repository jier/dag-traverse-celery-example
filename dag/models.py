from celery.backends.database.models import Task as CeleryTask
from typing import List
import networkx as nx
from networkx.algorithms.dag import is_directed_acyclic_graph
from sqlalchemy import ForeignKey, MetaData
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_json import MutableJson

Base = declarative_base()
class Workflow(Base):
    __tablename__ = 'workflow'
    id = Column(Integer, primary_key=True, autoincrement=True)
    dag_adjacency_list = Column(MutableJson, nullable=False, default={}) # definition of dependencies of children
    prio_type = Column(String(50), nullable=False, default='regular') # or scheduled
    status = Column(String(50), nullable=False, default='pending') # or success, failure
    tasks_status = Column(MutableJson, nullable=False, default={})
    children: Mapped[List["Task"]] = relationship("Task", back_populates="parent")

    @property
    def execution_graph(self):
        d = self.dag_adjacency_list
        G = nx.DiGraph()

        for node in d.keys():
            nodes = d[node]
            if len(nodes) == 0:
                G.add_node(int(node))
                continue
            G.add_edges_from([(int(node), n) for n in nodes])
        if is_directed_acyclic_graph(G):
            return G
        return None

    def to_dict(self):
            return {
                'id': self.id,
                'dag_adjacency_list': self.dag_adjacency_list,
                'prio_type': self.prio_type,
                'status': self.status,
                'tasks_status': self.tasks_status,
                'children': [child.to_dict() for child in self.children]
            }

    def count_children(self):
        return len(self.children)
    
    def get_child(self, child_id):
        return self.children[child_id]

    
    @classmethod
    def from_dict(cls, workflow_dict):
        children = []
        for child_dict in workflow_dict['children']:
            # Create an instance of the Task class for each dictionary
            child = Task.from_dict(child_dict)
            children.append(child)
        return cls(
            id=workflow_dict['id'],
            dag_adjacency_list= workflow_dict['dag_adjacency_list'],
            prio_type=workflow_dict['prio_type'],
            status=workflow_dict['status'],
            tasks_status=workflow_dict['tasks_status'],
            children=children
        )
    
    
class Task(Base):
    __tablename__ = 'task'
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id: Mapped[int] = mapped_column(ForeignKey("workflow.id"), nullable=True)
    parent :Mapped[Workflow] = relationship("Workflow", back_populates="children")
    celery_task_uid = Column(String(100))
    celery_task_status = Column(String(100),default='PENDING')  # or SUCCESS, FAILURE
    celery_task_retry_count = Column(Integer, nullable=True)
    sleep = Column(Integer, nullable=False)
    type = Column(String(50), nullable=False, default='pizza') # or pasta, burger, sushi
    dependencies = Column(MutableJson, nullable=True)  # can be null if no dependencies or dependencies in tasks that makes up this task


    def to_dict(self):
        return {
            'parent_id': self.parent_id,
            'celery_task_uid': self.celery_task_uid,
            'celery_task_status': self.celery_task_status,
            'celery_task_retry_count': self.celery_task_retry_count,
            'sleep': self.sleep,
            'type': self.type,
            'dependencies': self.dependencies
        }
    
    @classmethod
    def from_dict(cls, task_dict):
        return cls(
            parent_id=task_dict['parent_id'],
            celery_task_uid=task_dict['celery_task_uid'],
            celery_task_status=task_dict['celery_task_status'],
            celery_task_retry_count=task_dict['celery_task_retry_count'],
            sleep=task_dict['sleep'],
            type=task_dict['type'],
            dependencies=task_dict['dependencies']
        )
# TODO set Deployment to represent workflow, while a workflow can have multiple deployments
#  add revisions, status of deployment depending on status of workflow
# add method to get children status of workflow 