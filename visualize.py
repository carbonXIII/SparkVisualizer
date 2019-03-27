import networkx as nx
import matplotlib.pyplot as plt

import socket
from threading import Thread, Lock
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
            to_add = (dep, self)

            if not to_add in g.edges():
                g.add_edge(*to_add)

def server_loop(sock, rdds, rdds_lock):
    with sock:
        with sock.makefile('r') as f:
            for line in f:
                with rdds_lock:
                    if line.strip() == 'clear':
                        rdds.clear()
                        rdds['clear'] = True
                        continue

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

def run_server(port, rdds, rdds_lock):
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((socket.gethostname(), port))
    serversocket.listen(1)

    conn, addr = serversocket.accept()

    print('Connected at',addr)

    thread = Thread(target=server_loop, args=(conn, rdds, rdds_lock))
    thread.start()
    return thread

# wait for a connection
rdds = {}
rdds_lock = Lock()
server = run_server(8001, rdds, rdds_lock)

plt.ion()
plt.show()

g = nx.DiGraph()
while server.is_alive():
    with rdds_lock:
        if 'clear' in rdds:
            g.clear()
            del rdds['clear']

        plt.clf()
        for id in rdds:
            rdds[id].draw(g)
        pos = nx.drawing.nx_agraph.graphviz_layout(g, prog='dot')
        nx.draw(g, pos, with_labels=True, node_size=1000)
        plt.draw()
        plt.pause(2)
