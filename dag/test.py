# import pymysql 

# conn = pymysql.connect(host='localhost', user='admin', password='admin123', db='dag_celery', 
#                        charset='utf8mb4',local_infile=True,cursorclass=pymysql.cursors.DictCursor)
# cursor = conn.cursor()
# cursor.execute('SELECT * FROM task')
# print(cursor.fetchall())
# cursor.close()    
# docker run --name dag_celery_db -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root_password -e MYSQL_USER=admin -e MYSQL_PASSWORD=admin123 -e MYSQL_DATABASE=dag_celery mysql:8.0
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DATABASE_URI = 'mysql+pymysql://admin:admin123@localhost:3306/dag_celery?charset=utf8mb4'
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
# Test the connection
try:
    with engine.connect() as connection:
        result = connection.execute(text("show tables"))
        print(result.fetchone())
except Exception as e:
    print(f"Error: {e}")

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
# metadata_obj = MetaData()
class Workflow(Base):
    __tablename__ = 'workflow'
    # metadata_obj = metadata_obj
    id = Column(Integer, primary_key=True)
    dag_adjacency_list = Column(MutableJson)
    children: Mapped[List["Task"]] = relationship(back_populates="parent")

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
    # metadata_obj = metadata_obj
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id: Mapped[int] = mapped_column(ForeignKey("workflow.id"))
    parent :Mapped[Workflow] = relationship("Workflow", back_populates="children")
    celery_task_uid = Column(String(100))
    sleep = Column(Integer)
    # __mapper_args__ = {
    #     "polymorphic_identity": "task",
    #     # "inherit_condition": (celery_task_uid == CeleryTask.task_id),
    #     # "inherit_condition": (id == CeleryTask.id),

    # }

