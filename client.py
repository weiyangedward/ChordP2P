"""
A Client class, responsible for
1. init Chord node0
2. add new node
3. search in Chord
"""
import configreader
import xmlrpclib
import socket
from chordnode import ChordNode
import sys
import multiprocessing
import argparse
import re
import random
import time

m = 8

node_servers = {}
start_node_ip = 'http://localhost:' + str(configreader.get_starting_port())
start_node_server = xmlrpclib.ServerProxy(start_node_ip, verbose=False)
node_dict = dict()


def get_rpc_server(node_id):
    if node_id in node_servers:
        return node_servers[node_id]

    port_number = configreader.get_starting_port() + node_id
    node_address = 'http://localhost:' + str(port_number)

    server = None
    count = 10
    while server is None and count:
        count -= 1
        server = xmlrpclib.ServerProxy(node_address, verbose=False)
        if not server is None:
            break

    node_servers[node_id] = server
    return server


def is_server_connectable(node_id):
    server = get_rpc_server(node_id)
    try:
        server._()
    except xmlrpclib.Fault:
        pass
    except socket.error:
        return False

    return True


def show(node_id):
    if is_server_connectable(node_id):
        node_server = get_rpc_server(node_id)
        print node_server.show()
    else:
        print str(node_id),  ' does not exist'


def show_all():
    response = start_node_server.show_all()
    print response
    # total_message_sent()


def total_message_sent():
    message_sent = start_node_server.total_message_sent()
    print "total message sent: ", int(message_sent)


def crash(node_id):
    if node_id in node_dict and node_dict[node_id].is_alive():
        node_dict[node_id].terminate()
    else:
        print str(node_id),  ' does not exist'


def find(node_id, key):
    if node_id in node_dict and node_dict[node_id].is_alive():
        # server = node_dict[node_id]
        server = get_rpc_server(node_id)
    elif node_id == 0:
        server = start_node_server
    else:
        print str(node_id),  ' does not exist or crashed'
        return

    node_has_key = server.find_key(key)
    if node_has_key:
        print "node ", node_has_key, " found key ", key
    else:
        print "key ", key, " not found"

# join node
def join_node(node_id):
    node = ChordNode(node_id)
    node.start()
    node_dict[node_id] = node
    get_rpc_server(node_id)

def eval(eval_file):
    print "eval ", eval_file
    nodes, keys, trials = 0,0,0
    with open(eval_file, 'r') as FILE:
        for line in FILE:
            line = line.strip()
            tok, val = re.split(" ",line)
            if tok == "join":
                nodes = int(val)
            elif tok == "find":
                keys = int(val)
            elif tok == "trial":
                trials = int(val)
        FILE.close()

    node_added = []
    total_message_phase1, total_message_phase2 = 0, 0
    for trial in range(trials):
        print "trial ", trial
        message_phase1, message_phase2 = 0,0
        for i in range(nodes):
            print i, "=========================="
            while True:
                node_id = random.randint(1, 2**m-1)
                if not node_id in node_dict:
                    join_node(node_id)
                    node_added.append(node_id)
                    delay_time = random.uniform(1, 1)
                    time.sleep(delay_time)
                    break

        # wait for stabilize to finish
        time.sleep(1)

        message_phase1 = start_node_server.total_message_sent()
        print "message_phase1 ", message_phase1
        total_message_phase1 += int(message_phase1)

        for i in range(keys):
            node = random.randint(0, len(node_added) - 1)
            key = random.randint(1, 2**m-1)
            print "find node", node_added[node], "key ", key
            find(node_added[node], key)
            delay_time = random.uniform(0, 0.1)
            time.sleep(delay_time)

        message_phase2 = start_node_server.total_message_sent()
        total_message_phase2 += int(message_phase2)

    ave_total_message_phase1_per_node = total_message_phase1 / (trials * (nodes+1))
    ave_total_message_phase2_per_node = total_message_phase2 / (trials * (nodes+1))
    print "ave_total_message_phase1_per_node ", ave_total_message_phase1_per_node
    print "ave_total_message_phase2_per_node ", ave_total_message_phase2_per_node

    log_file = eval_file + "_log"
    with open(log_file, 'w') as LOG:
        LOG.write("ave_total_message_phase1_per_node " + str(ave_total_message_phase1_per_node) + "\n")
        LOG.write("ave_total_message_phase2_per_node " + str(ave_total_message_phase2_per_node) + "\n")
        LOG.close()
    print "eval finished"

def main():

    parser = argparse.ArgumentParser(description="client")
    parser.add_argument("--eval_file", dest = "eval_file", help="eval config file")
    args = parser.parse_args()
    if args.eval_file:
        eval(args.eval_file)

    while True:
        cmd = raw_input(">>")
        if cmd:
            args = cmd.split()
            # join [node]
            if args[0] == 'join':
                if args[1].isdigit():
                    node_id = int(args[1])
                    if 1 <= node_id <= 2**m-1:
                        print "join node", node_id
                        join_node(node_id)
                    else:
                        print "node out of range"
                else:
                    print "not know command"

            # find [node] [key]
            elif args[0] == 'find':
                node_id, key = int(args[1]), int(args[2])
                if 0 <= key <= 2**m-1:
                    find(node_id, key)
                else:
                    print "illegal key ", key

            # crash [node]
            elif args[0] == 'crash':
                node_id = int(args[1])
                crash(node_id)

            # show
            elif args[0] == 'show':
                # show all
                if args[1] == 'all':
                    show_all()
                # show [node]
                elif args[1].isdigit():
                    node_id = int(args[1])
                    if 0 <= node_id <= 2**m-1:
                        show(node_id)
                    else:
                        print "node out of range"
                else:
                    print "not know command"
            elif args[0] == "exit":
                break

            # wrong command
            else:
                print "Enter new command: "

if __name__ == '__main__':
    main()