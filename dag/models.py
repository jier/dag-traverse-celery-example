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
    dag_adjacency_list = Column(MutableJson, nullable=False, default={})
    prio_type = Column(String(50), nullable=False, default='regular') # or scheduled
    status = Column(String(50), nullable=False, default='pending') # or success, failure
    tasks_status = Column(MutableJson, nullable=False, default={})
    children: Mapped[List["Task"]] = relationship()

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

   
    def count_children(self):
        return len(self.children)
    
    


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
    dependencies = Column(MutableJson, nullable=True)  # can be null if no dependencies


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