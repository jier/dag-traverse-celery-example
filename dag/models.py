from celery.backends.database.models import Task as CeleryTask
from typing import List
# from .conf import DATABASE_URI
import networkx as nx
from networkx.algorithms.dag import is_directed_acyclic_graph
from sqlalchemy import ForeignKey, MetaData
from sqlalchemy import Column, Integer, String
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
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


class Task(Base):
    __tablename__ = 'task'
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id: Mapped[int] = mapped_column(ForeignKey("workflow.id"), nullable=True)
    parent :Mapped[Workflow] = relationship("Workflow", back_populates="children")
    celery_task_uid = Column(String(100))
    sleep = Column(Integer, nullable=False)
    type = Column(String(50), nullable=False, default='pizza') # or pasta, burger, sushi
    dependencies = Column(MutableJson, nullable=True)  # can be null if no dependencies
