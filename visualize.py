import networkx as nx
import matplotlib.pyplot as plt

import socket
from threading import Thread
from time import sleep

class RDD:
    def __init__(self, id, deps = []):
        self.id = id
        self.deps = deps

    def __str__(self):
        return 'RDD %d' % self.id

    def set_deps(self, deps):
        self.deps = deps

    def draw(self, g):
        for dep in self.deps:
            to_add = (self, dep)

            if not to_add in g.edges():
                g.add_edge(self, dep)

def server_loop(sock, rdds):
    with sock:
        with sock.makefile('r') as f:
            for line in f:
                words = line.split()

                id = int(words[0])
                deps = []

                if not id in rdds:
                    rdds[id] = RDD(id)
                    print('created',rdds[id])

                ndeps = int(words[1])
                for i in range(ndeps):
                    oid = int(words[2 + i])

                    if not oid in rdds:
                        rdds[oid] = RDD(oid)
                        print('created',rdds[oid])

                    print(rdds[id],'->',rdds[oid])

                    deps += [rdds[oid]]

                rdds[id].set_deps(deps)

def run_server(port, rdds):
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((socket.gethostname(), port))
    serversocket.listen(1)

    conn, addr = serversocket.accept()

    print('Connected at',addr)

    thread = Thread(target=server_loop, args=(conn, rdds))
    thread.start()
    return thread

# wait for a connection
rdds = {}
server = run_server(8001, rdds)

plt.ion()
plt.show()

g = nx.DiGraph()
while server.is_alive():
    plt.clf()
    for id in rdds:
        rdds[id].draw(g)
    pos = nx.circular_layout(g)
    nx.draw(g, pos, with_labels=True, node_size=1000)
    plt.draw()
    plt.pause(2)
