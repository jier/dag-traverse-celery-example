# # import pymysql 

# # conn = pymysql.connect(host='localhost', user='admin', password='admin123', db='dag_celery', 
# #                        charset='utf8mb4',local_infile=True,cursorclass=pymysql.cursors.DictCursor)
# # cursor = conn.cursor()
# # cursor.execute('SELECT * FROM task')
# # print(cursor.fetchall())
# # cursor.close()    
# # docker run --name dag_celery_db -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root_password -e MYSQL_USER=admin -e MYSQL_PASSWORD=admin123 -e MYSQL_DATABASE=dag_celery mysql:8.0
# from sqlalchemy import create_engine, text
# from sqlalchemy.orm import sessionmaker

# DATABASE_URI = 'mysql+pymysql://admin:admin123@localhost:3306/dag_celery?charset=utf8mb4'
# engine = create_engine(DATABASE_URI)
# Session = sessionmaker(bind=engine)
# # Test the connection
# try:
#     with engine.connect() as connection:
#         result = connection.execute(text("show tables"))
#         print(result.fetchone())
# except Exception as e:
#     print(f"Error: {e}")

# from celery.backends.database.models import Task as CeleryTask
# from typing import List
# # from .conf import DATABASE_URI
# import networkx as nx
# from networkx.algorithms.dag import is_directed_acyclic_graph
# from sqlalchemy import ForeignKey, MetaData
# from sqlalchemy import Column, Integer, String
# # from sqlalchemy import create_engine
# # from sqlalchemy.orm import sessionmaker
# from sqlalchemy.orm import relationship
# from sqlalchemy.orm import Mapped
# from sqlalchemy.orm import mapped_column
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy_json import MutableJson

# Base = declarative_base()
# # metadata_obj = MetaData()
# class Workflow(Base):
#     __tablename__ = 'workflow'
#     # metadata_obj = metadata_obj
#     id = Column(Integer, primary_key=True)
#     dag_adjacency_list = Column(MutableJson)
#     children: Mapped[List["Task"]] = relationship(back_populates="parent")

#     @property
#     def execution_graph(self):
#         d = self.dag_adjacency_list
#         G = nx.DiGraph()

#         for node in d.keys():
#             nodes = d[node]
#             if len(nodes) == 0:
#                 G.add_node(int(node))
#                 continue
#             G.add_edges_from([(int(node), n) for n in nodes])
#         if is_directed_acyclic_graph(G):
#             return G
#         return None


# class Task(Base):
#     __tablename__ = 'task'
#     # metadata_obj = metadata_obj
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     parent_id: Mapped[int] = mapped_column(ForeignKey("workflow.id"))
#     parent :Mapped[Workflow] = relationship("Workflow", back_populates="children")
#     celery_task_uid = Column(String(100))
#     sleep = Column(Integer)
#     # __mapper_args__ = {
#     #     "polymorphic_identity": "task",
#     #     # "inherit_condition": (celery_task_uid == CeleryTask.task_id),
#     #     # "inherit_condition": (id == CeleryTask.id),

#     # }

import random


# order_dependency = {
#     1: [3],
#     2: [3],
#     3: [4],
#     4: [5]

# }
# tastes = ['pizza', 'pasta', 'burger', 'sushi']
# pizza_order =['base', 'sauce', 'cheese', 'toppings']
# pasta_order = ['pasta', 'sauce', 'cheese', 'toppings']
# burger_order = ['bun', 'patty', 'cheese', 'toppings']
# sushi_order = ['rice', 'fish', 'seaweed', 'toppings']
# pizzas = ['margherita', 'pepperoni', 'hawaiian', 'meat feast']
# pastas = ['carbonara', 'bolognese', 'pesto', 'alfredo']
# burgers = ['cheeseburger', 'chicken burger', 'veggie burger', 'bacon burger']
# sushis = ['nigiri', 'sashimi', 'maki', 'temaki']
# wf_priority = ['regular','scheduled']
# wf_entries = []
# task_entries = []

# for i in range(1, 5):
#     task_entries.append([random.randint(1, 7),random.choice(tastes), order_dependency.get(i, [])])

# print(f"Task entries: {task_entries}")

# for i in wf_priority:
#     wf_entries.append([i, order_dependency])
# print(f"Workflow entries:{wf_entries}")
# pizza_order_dict = {idx: steps for idx, steps in enumerate(pizza_order)}


# pasta_order_dict = {idx: steps for idx, steps in enumerate(pasta_order)}
# burger_order_dict = {idx: steps for idx, steps in enumerate(burger_order)}
# sushi_order_dict = {idx: steps for idx, steps in enumerate(sushi_order)}
# print(f"Pizza order: {pizza_order_dict}")
# print(f"Pasta order: {pasta_order_dict}")
# print(f"Burger order: {burger_order_dict}")
# print(f"Sushi order: {sushi_order_dict}")

import networkx as nx
import matplotlib.pyplot as plt

# dag_adjacency_list_2 = {
#     0: [(2,)],
#     1: [(2,)],
#     2: [(3,)],
#     3: [(4)]
# }

# G = nx.DiGraph()

# for node, neighbors in dag_adjacency_list_2.items():
#     G.add_node(node)
#     for neighbor in neighbors:
#         G.add_edge(node, neighbor)
# nx.draw(G, with_labels=True)
# # pos = nx.spring_layout(G)
# # nx.draw_networkx_nodes(G, pos, node_size=500, node_color='lightgrey')
# # nx.draw_networkx_edges(G, pos)
# # nx.draw_networkx_labels(G, pos)

# plt.axis('off')
# plt.show()

# Define the adjacency lists
make_dependency_1 = {
    1: [3],
    2: [4],
    3: [5],
    4: [5],
    5: [6, 7],
    6: [8],
    7: [8]
}
# make_dependency_1 = {
#     1: [3],
#     2: [3],
#     3: [4],
#     4: []
# }
make_dependency_2 = {
    1: [3],
    2: [3],
    3: [4],
    4: [5]
}

# Create a directed graph for each adjacency list
G1 = nx.DiGraph()
G2 = nx.DiGraph()
G3 = nx.DiGraph()
# Add edges to the graphs
for node, successors in make_dependency_1.items():
    for successor in successors:
        G1.add_edge(node, successor)

for node, successors in make_dependency_2.items():
    for successor in successors:
        G2.add_edge(node, successor)

d = make_dependency_1
for node in d.keys():
    # print('node:', node)
    nodes = d[node]
    # print('nodes:', nodes)
    if len(nodes) == 0:
        G3.add_node(int(node))
        continue
    G3.add_edges_from([(int(node), n) for n in nodes])
if nx.is_directed_acyclic_graph(G3):
    print('G3 is a DAG')
else: 
    print('G3 is not a DAG')      
# # Plot the graphs singularly
# pos = nx.spring_layout(G1)
# nx.draw(G1, pos, with_labels=True, node_color='lightblue', node_size=1500, edge_color='gray', linewidths=1, font_size=12)
# plt.show()

# pos = nx.spring_layout(G2)
# nx.draw(G2, pos, with_labels=True, node_color='lightblue', node_size=1500, edge_color='gray', linewidths=1, font_size=12)
# plt.show()

# Plot the graphs side by side
# pos1 = nx.spring_layout(G1)
# pos2 = nx.spring_layout(G2)

# # Create a new figure with subplots
# fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6))

# # Plot G1 on the left subplot
# ax1.axis('off')
# nx.draw(G1, pos1, ax=ax1, with_labels=True, node_color='lightblue', node_size=1500, edge_color='gray', linewidths=1, font_size=12 )
# nx.draw_networkx_nodes(G1, pos1, ax=ax1, node_color='lightblue', node_size=1500)
# nx.draw_networkx_labels(G1, pos1, ax=ax1, font_size=12)
# nx.draw_networkx_edges(G1, pos1, ax=ax1, edge_color='gray', width=1, arrowsize=20, arrows=True)

# # Plot G2 on the right subplot
# ax2.axis('off')
# nx.draw(G2, pos2, ax=ax2, with_labels=True, node_color='lightblue', node_size=1500, edge_color='gray', linewidths=1, font_size=12)
# nx.draw_networkx_nodes(G2, pos2, ax=ax2, node_color='lightgreen', node_size=1500)
# nx.draw_networkx_labels(G2, pos2, ax=ax2, font_size=12)
# nx.draw_networkx_edges(G2, pos2, ax=ax2, edge_color='gray', width=1, arrowsize=20, arrows=True)

# # Layout so plots do not overlap
# fig.tight_layout()

# plt.show()
# Check if the graphs are DAGs
# print("Is G1 a DAG?", nx.is_directed_acyclic_graph(G1))
# print("Is G2 a DAG?", nx.is_directed_acyclic_graph(G2))

# print(nx.topological_sort(G1))

#     if len(list(G1.predecessors(node))) == 0:
#         result.append(node)
#     else:
#         result.append(list(G1.successors(node)))

# print(f"Result entrypoint: {result}")
data = [{'parent_id': 2, 'celery_task_uid': '6e278c68-46fb-4465-b1d8-846b48ee593e', 'celery_task_status': 'SUCCESS', 'celery_task_retry_count': None, 'sleep': 2, 'type': 'pesto', 'dependencies': {}}, {'parent_id': 2, 'celery_task_uid': '0ff8b31e-8f5c-41c3-a125-d3d4d0485b65', 'celery_task_status': 'SUCCESS', 'celery_task_retry_count': None, 'sleep': 2, 'type': 'carbonara', 'dependencies': {}}, {'parent_id': 2, 'celery_task_uid': '25e589b3-ce71-4c41-bcfd-1207af900ebd', 'celery_task_status': 'SUCCESS', 'celery_task_retry_count': None, 'sleep': 3, 'type': 'pesto', 'dependencies': {}}, {'parent_id': 2, 'celery_task_uid': 'c5d62fc2-1d95-4076-92bb-dbb407cedb19', 'celery_task_status': 'SUCCESS', 'celery_task_retry_count': None, 'sleep': 2, 'type': 'pesto', 'dependencies': {}}]
print(data[0]['parent_id'])