import threading
import xmlrpclib
import socket
import time
import configreader
import random


STABLIZE_PERIOD = 5


class Stabilizer(threading.Thread):

    def __init__(self, node_id):
        super(Stabilizer, self).__init__()
        self.port_number = configreader.get_starting_port() + node_id
        self.node_address = 'http://localhost:' + str(self.port_number)
        # self.rpc_server = xmlrpclib.ServerProxy(self.node_address, verbose=False)
        self.min_delay, self.max_delay = configreader.get_delay_info()


    def run(self):
        while True:
            delay_time = random.uniform(4, 5)
            time.sleep(delay_time)
            self.stabilize()

    def stabilize(self):
        rpc_server = None
        count = 10
        while rpc_server is None and count:
            count -=1
            try:
                rpc_server = xmlrpclib.ServerProxy(self.node_address, verbose=False)
                if not rpc_server is None:
                    break
            except:
                pass
        rpc_server.stabilize()

    def is_server_connectable(self):
        server = self.rpc_server
        try:
            server._()
        except xmlrpclib.Fault:
            pass
        except socket.error:
            return False

        return True