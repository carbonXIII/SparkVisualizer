import networkx as nx
import matplotlib.pyplot as plt

import socket
from threading import Thread, Lock
from time import sleep

class RDD:
    def __init__(self, id, deps = []):
        self.id = id
        self.deps = deps
        self.highlighted = False

    def __str__(self):
        return '%d' % self.id

    def set_deps(self, deps):
        self.deps = deps

    def draw(self, g):
        for dep in self.deps:
            to_add = (dep, self)

            if not to_add in g.edges():
                g.add_edge(*to_add)

def server_loop(sock, rdds, edit_lock):
    with sock:
        with sock.makefile('r') as f:
            for line in f:
                with edit_lock:
                    words = line.split()

                    if words[0].strip() == 'clear':
                        rdds.clear()
                        print('clear')
                        rdds['clear'] = True
                    elif words[0].strip() == 'highlight':
                        n = int(words[1])

                        to_highlight = [int(words[2 + i]) for i in range(n)]
                        for id in rdds:
                            if type(id) is int:
                                rdds[id].highlighted = id in to_highlight
                    else:
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

def run_server(port, rdds, edit_lock):
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((socket.gethostname(), port))
    serversocket.listen(1)

    while True:
        conn, addr = serversocket.accept()

        print('Connected at',addr)

        Thread(target=server_loop, args=(conn, rdds, edit_lock)).start()


rdds = {}
edit_lock = Lock()
server_thread = Thread(target=run_server, args=(8001, rdds, edit_lock))
server_thread.start()

plt.ion()
plt.show()

color_map = []
g = nx.DiGraph()
while server_thread.is_alive():
    with edit_lock:
        if 'clear' in rdds:
            g.clear()
            del rdds['clear']

        plt.clf()
        color_map = []
        for id in rdds:
            rdds[id].draw(g)

        for node in g.nodes():
            color_map += ['blue' if node.highlighted else 'red']

    pos = nx.drawing.nx_agraph.graphviz_layout(g, prog='dot')
    nx.draw(g, pos, node_color = color_map, with_labels=True, node_size=1000)
    plt.draw()
    plt.pause(2)
