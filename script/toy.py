import os, sys
import argparse

DIR_OUT = '../data/toy_graphs'
SHORT_OUTPUT = True

def gfile(graph_name):
    path = os.path.join(DIR_OUT, graph_name, 'edgelist.bin')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, 'wb')


def write_edge(f, u, v):
    if SHORT_OUTPUT:
        f.write(int.to_bytes(u, 4, 'little'))
        f.write(int.to_bytes(v, 4, 'little'))
    else:
        f.write(int.to_bytes(u, 8, 'little'))
        f.write(int.to_bytes(v, 8, 'little'))

def write_edges(f, edges):
    for u, v in edges:
        write_edge(f, u, v)

def complete_graph(vertices):
    return [(u, v) for u in vertices for v in vertices if u != v]


os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.makedirs(DIR_OUT, exist_ok=True)

n = 1000
with gfile(f'chain{n}') as f:
    for i in range(n):
        write_edge(f, i, i+1)


n = 20
with gfile(f'K{n}') as f:
    for i in range(n):
        for j in range(n):
            if i != j:
                write_edge(f, i, j)

n = 50
m = 1000
with gfile(f'K{n}x{m}') as f:
    for k in range(m):
        st, ed = k*n, (k+1)*n
        for i in range(st, ed):
            for j in range(st, ed):
                if i != j:
                    write_edge(f, i, j)


n = 20
with gfile(f'K{n}-D') as f:
    for i in range(n):
        for j in range(i+1, n):
            write_edge(f, i, j)

level = 14
with gfile(f'binarytree{level}') as f:
    n = 2**level
    for i in range(n-1):
        write_edge(f, i, i*2)
        write_edge(f, i, i*2+1)

batch = 128
step = 64
with gfile(f'batch_circle-128-64') as f:
    last_vertices = list(range((step-1)*batch, step*batch))
    for i in range(step):
        vertices = list(range(i*batch, (i+1)*batch))
        edges = complete_graph(vertices)
        write_edges(f, edges)
        edges = [(u, v) for u in last_vertices for v in vertices]
        write_edges(f, edges)
        last_vertices = vertices


n = 1024
k = 32
# 所有节点都有K重自环，用于测试，可以从目的顶点判断源顶点是否正确
with gfile(f'selfloop{n}-{k}') as f:
    for i in range(n):
        for j in range(k):
            write_edge(f, i, i)


n = 64
k = 32
# 所有节点都有K重自环，用于测试，可以从目的顶点判断源顶点是否正确
with gfile(f'selfloop{n}-{k}') as f:
    for i in range(n):
        for j in range(k):
            write_edge(f, i, i)
