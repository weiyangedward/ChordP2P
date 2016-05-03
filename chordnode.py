"""
A Chord node class
"""

import configreader
from collections import namedtuple
from SimpleXMLRPCServer import SimpleXMLRPCServer
import multiprocessing
import threading
import xmlrpclib
import socket
import time
from stabilizer_process import Stabilizer
import random

# allow concurrent rpc request
import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

# delay: use thread to get_rpc_server() and return value
from multiprocessing.pool import ThreadPool
pool = ThreadPool(processes=100)


m = 8  # node identifiers and keys both contain 8 bits
start_node_ip = 'http://localhost:' + str(configreader.get_starting_port())
FingerTableEntry = namedtuple('FingerTableEntry', 'start node')


# class to fix concurrent
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer):
    pass


class ChordNode(multiprocessing.Process):

    def __init__(self, node_id):
        super(ChordNode, self).__init__()

        self.id = node_id

        self.message_sent = 0

        self.port_number = configreader.get_starting_port() + self.id
        self.start_node = True if node_id == 0 else False

        self.min_delay, self.max_delay = configreader.get_delay_info()

        self.fingers = [None] * m
        self.predecessor = None

        self.successor = [0, 0]  # this successor list contains two successors
        self.keys = set()
        self.predecessor_keys = set()

        self.connected_rpc_servers = {}
        self.predecessor_crashed = False

        self.lock = multiprocessing.Lock()

        # init chord node
        if self.start_node:
            for x in range(256): 
                self.keys.add(str(x))
            self.init_finger_table()
        else:
            self.start_node_server = xmlrpclib.ServerProxy(start_node_ip)
            self.connected_rpc_servers[0] = self.start_node_server
            self.join_node()

        self.stabilizer = Stabilizer(self.id)

    # Step0: join a new node
    def join_node(self):
        print self.id, "join_node---------------"
        self.init_finger_table()
        self.update_others()
        self.get_keys()
        self.get_duplicate_key_from_predecessor()
        self.notify_successor_its_keys()
        self.init_successor_list()
        print "join_node finished"

    # Step1: init_finger_table
    def init_finger_table(self):
        # print self.id, "init_finger_table----------------"

        # if the node 0
        if self.start_node:
            for i in range(0, m):
                start = (self.id + 2**i) % (2 ** m)
                finger_table_entry = FingerTableEntry(start=start, node=self.id)
                self.fingers[i] = finger_table_entry
            self.predecessor = self.id

        # if a new node
        else:
            # ask node 0 to find its successor
            start = (self.id + 1) % (2 ** m)

            # find successor of n
            successor = self.get_rpc_server(0).find_successor(start)
            finger_table_entry = FingerTableEntry(start=start, node=successor)
            self.fingers[0] = finger_table_entry

            # get predecessor from n's successor
            self.predecessor = self.get_rpc_server(self.fingers[0].node).get_predecessor()

            # set n's successor's predecessor = n
            self.get_rpc_server(self.fingers[0].node).set_predecessor(self.id)

            # update the rest of finger table start from i= 1 to m-1
            for i in range(1, m):
                start = (self.id + 2**i) % (2 ** m)
                if self.id <= start < self.fingers[i-1].node:
                    node = self.fingers[i-1].node
                elif start == 0:
                    node = 0  # because node 0 will not crash, for start with value 0, node 0 will always be its node.
                else:
                    node = self.get_rpc_server(0).find_successor(start)

                if node < start < self.id:
                    node = self.id
                finger_table_entry = FingerTableEntry(start=start, node=node)
                self.fingers[i] = finger_table_entry
        # print "init_finger_table finished"

    # Step2: update all nodes whose fingers point n
    def update_others(self):
        # print self.id, "update_others ------------"
        for i in range(0, m):
            id = (self.id - 2 ** i) % (2 ** m)
            # print "id = ", id
            if self.is_server_connectable(id):
                # print 'connected', id
                self.get_rpc_server(id).update_finger_table(self.id, i)
                # self.get_rpc_server(p).update_finger_table(self.id, i)
            else:
                # if the nodes has not joined the network so far
                # we ask node 0 for its predecessor
                p = self.get_rpc_server(0).find_predecessor(self.id, id)

                # print id, ' predecessor is', p

                if p != self.id:
                    self.get_rpc_server(p).update_finger_table(self.id, i)
        # print "update finish"

    # Step3: newly joined node get keys from its successor
    def get_keys(self):
        # print self.id, "get_keys----------------"
        successor = self.fingers[0].node
        receive_keys = self.get_rpc_server(successor).transfer_keys_to(self.id, self.predecessor)
        for key in receive_keys:
            self.keys.add(key)
        # print "get_keys finished"

    # Step4: all chord node add successor list, successor_1 = n's successor, successor_2 = 0
    def init_successor_list(self):
        # print self.id, "init_successor_list ----------------"
        self._add_successor(self.fingers[0].node, 0)
        # print "init_successor_list finished"

    # add successor_1, successor_2 to successor_list
    def _add_successor(self, successor_1, successor_2):
        # print self.id, "_add_successor...", successor_1, " and ", successor_2
        self.successor[0] = successor_1
        self.successor[1] = successor_2
        # print self.successor

    # if s is ith finger of n, update n's finger table with s
    def update_finger_table(self, s, i):
        # print s, " update ", self.id, " finger_table ",  i, "-----------"
        # print 'before update_finger_table:'
        # print self.fingers
        # print self._beautify_output()

        if self.id > self.fingers[i].node:
            # print "node ", self.id, " > ", self.fingers[i].node
            if self.id <= s < 2 ** m or 0 < s < self.fingers[i].node:

                finger_entry = FingerTableEntry(start=self.fingers[i].start, node=s)
                self.fingers[i] = finger_entry

                # update successor list
                if i == 0:
                    self.successor[0] = s

                if not s == self.predecessor:
                    # print "not ",  s, " == ", self.predecessor
                    self.get_rpc_server(self.predecessor).update_finger_table(s, i)

            elif s == self.fingers[i].start:

                finger_entry = FingerTableEntry(start=self.fingers[i].start, node=s)
                self.fingers[i] = finger_entry

                if i == 0:
                    self.successor[0] = s

                # print self.fingers
        else:
            # print "node ", self.id, " <= ", self.fingers[i].node
            if (self.id <= s < self.fingers[i].node) or (self.id == self.fingers[i].node) or (s < self.fingers[i].node):

                finger_entry = FingerTableEntry(start=self.fingers[i].start, node=s)
                self.fingers[i] = finger_entry

                if i == 0:
                    self.successor[0] = s

                if not s == self.predecessor:
                    # print "not ",  s, " == ", self.predecessor
                    self.get_rpc_server(self.predecessor).update_finger_table(s, i)
                # print self.fingers

        # print 'after update finger tables:'
        # print self.fingers
        # print self._beautify_output()
        return True

    def to_chord_node(self, node_id):
        return int(node_id) % 2**m

    # transfer keys to a newly joined node
    def transfer_keys_to(self, n, n_predecessor):
        # print self.id, " transfer_keys_to ", n, ' with predecessor', n_predecessor

        self.predecessor_keys = set()
        send_keys = []
        keys_to_remove = []
        predecessor = n_predecessor
        low = predecessor
        up = n

        for key in self.keys:
            if low < int(key) <= up:
                send_keys.append(str(key))
                keys_to_remove.append(key)

        for key in keys_to_remove:
            self.keys.remove(str(key))

        return send_keys

    def find_predecessor(self, caller, id):
        return self._find_predecessor(caller, id)

    def _find_predecessor(self, caller, id):

        if self.id == id == 0:
            return self.predecessor

        node = self.id

        if self.fingers[0].node == 0:
            node_successor = 0
        else:
            # print "not self.fingers[0].node == 0:"
            if self.fingers[0].node == caller:

                node_successor = self.fingers[0].node
            else:
                if self.is_server_connectable(self.fingers[0].node):
                    # print "self.is_server_connectable(self.fingers[0].node):"
                    node_successor = self.fingers[0].node
                else:
                    # print "not self.is_server_connectable(self.fingers[0].node):"
                    node_successor = self.fingers[1].node

        while True:
            # print "node ", node, " node_successor" , node_successor, " target ", id
            # node = 1, suc = 0
            low, up = 0,0
            if node_successor < node:
                low, up = node_successor, node
                if not low <= id < node:
                    return node
                else:
                    if node == self.id:
                        node = self.find_closet_preceding_finger(id)

                    else:
                        if node == 0:
                            node = self.find_closet_preceding_finger(id)
                        else:
                            node = self.get_rpc_server(node).find_closet_preceding_finger(id)


                    if not self.is_server_connectable(node):
                        # print node, "not connected"
                        return node

                    if node == 0:
                        node_successor = self.get_successor()
                    else:
                        node_successor = self.get_rpc_server(node).get_successor()

                    # print 'if not node_successor: ', node_successor
            else:
                # print "successor ", node_successor , " >= ", node
                if not (node < id <= node_successor or node == node_successor):
                    if node == self.id:
                        node = self.find_closet_preceding_finger(id)

                    else:
                        if node == 0:
                            node = self.find_closet_preceding_finger(id)
                        else:
                            node = self.get_rpc_server(node).find_closet_preceding_finger(id)

                    if not self.is_server_connectable(node):
                        # print node, "not connected"
                        return node

                    if node == 0:
                        node_successor = self.get_successor()
                    else:
                        node_successor = self.get_rpc_server(node).get_successor()

                    # print 'if not node_successor: ', node_successor
                else:
                    break
        # print 'find predecessor node', node
        return node

    # return closet finger preceding id
    def find_closet_preceding_finger(self, id):
        # loop from (m-1) to 0
        # print self.id, 'find_closet_preceding_finger...'
        ans = self.id
        for i in range(m - 1, -1, -1):
            # print i, "-th finger node", self.fingers[i].node, " target ", id
            low, up = 0,0
            if self.id > id:
                # print "self.id > id"
                low, up = id, self.id
                if not low < self.fingers[i].node <= up:
                    ans = self.fingers[i].node
                    break
            else:
                # print "self.id <= id"
                # if self.id < self.fingers[i].node < id:
                low, up = self.id, id
                if low < self.fingers[i].node < up:
                    ans=  self.fingers[i].node
                    break
        # print 'return find_closet_preceding_finger:', ans
        return ans

    # return node n's successor
    def find_successor(self, n):
        # print self.id, " find_successor for ", n

        n_predecessor = self._find_predecessor(self.id, n)

        if n_predecessor == self.id:
            # print "n_predecessor ", n_predecessor, " == ", self.id
            return self.fingers[0].node
        else:
            # print "not n_predecessor ", n_predecessor, " == ", self.id
            if self.is_server_connectable(n_predecessor):
                # print n_predecessor, "is connected"
                return self.get_rpc_server(n_predecessor).get_successor()
            else:
                pass
                # print n_predecessor, " not response"
        return None

    def get_successor(self):
        # print self.id, "get_successor ---------"
        successor = None
        if not self.successor[0] == self.id:
            successor = self.successor[0]
        elif not self.successor[1] == self.id:
            successor = self.successor[1]
        # print self.id, " successor is ", successor
        return successor

    def get_predecessor(self):
        return self.predecessor

    # set predecessor to node n
    def set_predecessor(self, n):
        self.predecessor = n
        if n != 0:
            self.get_duplicate_key_from_predecessor()
        return True

    # init thread to add delay to get_rpc_server
    def get_rpc_server_delay(self, id):
        delay_server = pool.apply_async(self.get_rpc_server_without_delay, (id, True))
        server = delay_server.get(timeout=None)
        return server

    def get_rpc_server(self, id):

        # if delay

        # if self.id == 0:
        #     return self.get_rpc_server_without_delay(id)
        # else:
        #     return self.get_rpc_server_delay(id)

        # no delay
        return self.get_rpc_server_without_delay(id)

    def get_rpc_server_without_delay(self, id, delay=False):
        # print self.id, " get_rpc_server ", id

        if delay:
            delay_time = random.uniform(self.min_delay, self.max_delay)
            # delay_time = 0
            # print "delay ", delay_time
            time.sleep(delay_time)

        server = None
        count = 10
        while server is None and count:
            count -= 1
            try:
                port_number = configreader.get_starting_port() + id
                node_address = 'http://localhost:' + str(port_number)
                server = xmlrpclib.ServerProxy(node_address)
                # print self.id, "server ", server
                self.connected_rpc_servers[id] = server
                self.message_sent += 1
                if not server is None:
                    return server
            except:
                print "cannot connect to server ", id
        return None

    # http://stackoverflow.com/questions/4716598/safe-way-to-connect-to-rpc-server
    def is_server_connectable(self, id):
        server = self.get_rpc_server(id)
        try:
            server._()
        except xmlrpclib.Fault:
            pass
        except socket.error:
            return False
        return True

    # pack fingers and keys into a str
    def _beautify_output(self):
        output = ''
        output += str(self.id) + '\n'

        output += 'FingerTable:'

        finger_str_list = []
        for i, finger in enumerate(self.fingers):
            finger_str = 'f' + str(i + 1)
            finger_str += '['
            finger_str += 'start:' + str(finger.start) + ' node:' + str(finger.node)
            finger_str += ']'
            finger_str_list.append(finger_str)

        output += ','.join(finger_str_list) + '\n'

        keys_list = list(self.keys)
        keys_list.sort(key=float)

        output += 'Keys: '
        for key in keys_list:
            output += str(key) + ' '
        #
        keys_list = list(self.predecessor_keys)
        keys_list.sort(key=float)
        output += 'Predecessor keys: '
        for key in keys_list:
            output += str(key) + ' '

        output += str(self.successor)
        return output

    def show(self):
        return self._beautify_output()

    # show all nodes finger table and keys
    def show_all(self):
        # print "show_all------------------"
        all_node_finger_str = []
        all_node_finger_str.append(self._beautify_output())

        successor = self.get_successor()

        while successor and self.id != successor:
            if self.is_server_connectable(successor):
                all_node_finger_str.append(self.get_rpc_server(successor).show())
                successor = self.get_rpc_server(successor).get_successor()
            else:
                break

        return '\n\n'. join(all_node_finger_str)

    # find [key]
    def find_key(self, key):
        # print "node ", self.id, " finding key ", key

        finger_node_found = False

        if str(key) in self.keys:
            return str(self.id)
        else:
            if self.id < key <= self.fingers[0].node:
                # print "self.id < key <= self.fingers[0].node"
                return self.get_rpc_server(self.fingers[0].node).find_key(key)
            else:
                # print "not self.id < key <= self.fingers[0].node"
                # print key, " not in node ", self.id
                for i in range(m):
                    # print "check ", i, "-th finger node ", self.fingers[i].node
                    if self.fingers[i].node <= key:
                        finger_node_found = True
                        # print "finger node ", self.fingers[i].node, " <= ", key
                        # if key not exist in the system

                        # if self.fingers[i].node == self.id:
                        #     return None
                        # else:

                        if self.fingers[i].node == 0 and not self.id == 0:
                            return self.get_rpc_server(0).find_key(key)
                        elif self.fingers[i].node == 0 and self.id == 0:
                            return self.find_key(key)
                        else:
                            return self.get_rpc_server(self.fingers[i].node).find_key(key)
                        break
                    else:
                        pass
                        # print  "finger node ", self.fingers[i].node, " > ", key
        if not finger_node_found:
            return self.get_rpc_server(self.successor[0]).find_key(key)
        return None

    def get_message_sent(self):
        return str(self.message_sent)

    # count total number of messages sent in chord
    def total_message_sent(self):
        print "total_message_sent----------------"
        print "self.successor[0] ", self.successor[0]
        if self.is_server_connectable(self.successor[0]):
            successor = self.successor[0]
        else:
            successor = self.successor[1]
        print "successor ", successor

        total_m = 0
        total_m += int(self.get_message_sent())

        while self.id != successor:
            total_m += int(self.get_rpc_server(successor).get_message_sent())
            successor = self.get_rpc_server(successor).get_successor()

        return str(total_m)

    # =========================================
    # Stabilization related code goes from here
    # =========================================
    def stabilize(self):
        # print self.id, "stabilize actual function ------"
        # check if its successor exists
        # if exits and the successor is not node 0, ask successor's successor
        # otherwise, we will handle successor crash
        if self.predecessor_crashed:
            self.predecessor_crashed = False
            self.get_duplicate_key_from_predecessor()
            # when its keys got updated
            if self.successor[0] != self.id and self.id != 0:
                self.notify_successor_its_keys()

        if self.successor[0] != self.id and self.successor[0] != 0:
            if self.is_server_connectable(self.successor[0]):
                self.successor[1] = self.get_rpc_server(self.successor[0]).get_successor()
            else:
                # successor crash
                # print 'self.predecessor, ', self.predecessor
                # when it is node 0
                if self.predecessor == self.successor[0]:
                    self.predecessor = self.id
                    self._fix_own_finger_table()
                    self.keys = self.keys | self.predecessor_keys
                    self.predecessor_keys = set()
                    self.successor[0] = self.successor[1]
                else:
                    self._fix_own_finger_table()
                    self.get_rpc_server(self.predecessor).fix_finger_table(self.id, self.successor[0], self.successor[1])
                    self.successor[0] = self.successor[1]

        else:
            pass

    def _fix_own_finger_table(self):
        for i, finger in enumerate(self.fingers):
            if finger.node == self.successor[0]:
                finger_table_entry = FingerTableEntry(start=finger.start, node=self.successor[1])
                self.fingers[i] = finger_table_entry

    def fix_finger_table(self, from_node, old_node, new_node):
        for i, finger in enumerate(self.fingers):
            if finger.node == old_node:
                finger_table_entry = FingerTableEntry(start=finger.start, node=new_node)
                self.fingers[i] = finger_table_entry

        if self.id != new_node:
            self.get_rpc_server(self.predecessor).fix_finger_table(from_node, old_node, new_node)
        else:
            self.predecessor = from_node
            self.keys = self.keys | self.predecessor_keys
            self.predecessor_crashed = True

    def get_keys_for_stabilization(self):
        keys = []
        for key in self.keys:
            keys.append(str(key))
        return keys

    def get_duplicate_key_from_predecessor(self):
        # if this node's predecessor equal to 0, we know it will never crash
        if self.predecessor == 0:
            pass
        else:
            if self.is_server_connectable(self.predecessor):
                predecessor_keys = self.get_rpc_server(self.predecessor).get_keys_for_stabilization()
                self.predecessor_keys = set()
                for key in predecessor_keys:
                    self.predecessor_keys.add(str(key))
                # print 'get predecessor_keys', self.predecessor_keys
            else:
                pass
                # print 'get_duplicate_key_from_predecessor predecessor crash'

    def notify_successor_its_keys(self):
        successor = self.fingers[0].node
        if self.id != successor:
            if self.is_server_connectable(successor):
                send_keys = []
                for key in self.keys:
                    send_keys.append((str(key)))
                self.get_rpc_server(successor).receive_keys_from_predecessor(send_keys)
            else:
                print 'notify_successor_its_keys successor crash'

    def receive_keys_from_predecessor(self, keys):
        self.predecessor_keys = set()
        for key in keys:
            self.predecessor_keys.add(key)
        return True

    def run(self):
        # server = SimpleXMLRPCServer(('localhost', self.port_number), logRequests=False, allow_none=True)
        server = AsyncXMLRPCServer(('', self.port_number), SimpleXMLRPCRequestHandler, logRequests=False,
                                   allow_none=True)
        server.register_introspection_functions()

        # register RPC calls
        # RPC calls for communication between nodes
        server.register_function(self.find_successor, 'find_successor')
        server.register_function(self.find_predecessor, 'find_predecessor')
        server.register_function(self.get_successor, 'get_successor')
        server.register_function(self.get_predecessor, 'get_predecessor')
        server.register_function(self.set_predecessor, 'set_predecessor')
        server.register_function(self.update_finger_table, 'update_finger_table')
        server.register_function(self.find_closet_preceding_finger, 'find_closet_preceding_finger')
        server.register_function(self.transfer_keys_to, 'transfer_keys_to')
        server.register_function(self.init_successor_list, 'init_successor_list')
        server.register_function(self.stabilize, 'stabilize')
        server.register_function(self.get_message_sent, 'get_message_sent')

        # RPC calls for stabilization
        server.register_function(self.get_keys_for_stabilization, 'get_keys_for_stabilization')
        server.register_function(self.receive_keys_from_predecessor, 'receive_keys_from_predecessor')
        server.register_function(self.fix_finger_table, 'fix_finger_table')

        # RPC calls for client
        server.register_function(self.find_key, 'find_key')
        server.register_function(self.show, 'show')
        server.register_function(self.show_all, 'show_all')
        server.register_function(self.total_message_sent, 'total_message_sent')

        # self.stabilizer.start()

        server.serve_forever()