from celery.backends.database.models import Task as CeleryTask
from typing import List
# from .conf import DATABASE_URI
import networkx as nx
from networkx.algorithms.dag import is_directed_acyclic_graph
from sqlalchemy import ForeignKey, MetaData, Table
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_json import MutableJson
from sqlalchemy import String, Boolean

Base = declarative_base()
metadata = MetaData()

# Many-to-many association table
# deployment_task = Table(
#     'deployment_task', metadata,
#     Column('deployment_id', Integer, ForeignKey('deployment.id')),
#     Column('task_id', Integer, ForeignKey('task.id'))
# )
class Workflow(Base):
    __tablename__ = 'workflow'
    id = Column(Integer, primary_key=True)
    dag_adjacency_list = Column(MutableJson, nullable=False, default={})
    prio_type = Column(String(50), nullable=False, default='regular') # or scheduled
    tasks_status = Column(MutableJson, nullable=False)
    feedback_buckets = Column(MutableJson, nullable=False)
    target_system_id = Column(Integer, nullable=True)  # can be null if not specified
    total_status_runs = Column(MutableJson, nullable=False)
    children: Mapped[List["Task"]] = relationship(back_populates="parent")
    deployments = relationship("Deployment", back_populates="workflow")
    deployment_tasks: Mapped[List["DeploymentTask"]] = relationship(back_populates="workflow")

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

class Task(Base):
    __tablename__ = 'task'
    id = Column(Integer, primary_key=True, autoincrement=True)
    workflow_id: Mapped[int] = mapped_column(ForeignKey("workflow.id", ondelete="CASCADE"), nullable=False)
    parent :Mapped[Workflow] = relationship("Workflow", back_populates="children")
    celery_task_uid = Column(String(100), unique=True,nullable=False)
    sleep = Column(Integer, nullable=False)
    type = Column(String(50), nullable=False, default='pizza') # or pasta, burger, sushi
    dependencies = Column(MutableJson, nullable=True)  # can be null if no dependencies
    feedback_bucket = Column(String(50), nullable=True)  # can be null if not specified
    target_system_id = Column(Integer, nullable=True)  # can be null if not specified

# Workflow.deployments = relationship("Deployment", back_populates="workflow")
class Deployment(Base):
    __tablename__ = 'deployment'
    id = Column(Integer, primary_key=True)
    workflow_id: Mapped[int] = mapped_column(ForeignKey("workflow.id", ondelete="CASCADE"), nullable=False)
    workflow: Mapped[Workflow] = relationship(back_populates="deployments")
    tasks_status = Column(MutableJson, nullable=False)
    retry_counts = Column(MutableJson, nullable=False)
    tasks = tasks = relationship("DeploymentTask", back_populates="deployment")

    @property
    def task_ids(self):
        return list(self.tasks_status.keys())

# Task.deployments = relationship("Deployment", secondary=deployment_task, back_populates="tasks")

class DeploymentTask(Base):
    __tablename__ = 'deployment_task'
    id = Column(Integer, primary_key=True)
    deployment_id: Mapped[int] = mapped_column(ForeignKey("deployment.id", ondelete="CASCADE"), nullable=False)
    task_id: Mapped[int] = mapped_column(ForeignKey("task.id", ondelete="CASCADE"), nullable=False)
    status = Column(String(50), nullable=False, default='pending') # or regular, failed, success
    retry_count = Column(Integer, nullable=False, server_default='0', default=0)
    workflow_id: Mapped[int] = mapped_column(ForeignKey("workflow.id", ondelete="CASCADE"), nullable=False)
    workflow: Mapped[Workflow] = relationship(back_populates="deployment_tasks")
