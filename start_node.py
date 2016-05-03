from chordnode import ChordNode
from multiprocessing import Process
import time
import sched
import multiprocessing
import threading

# initialize the system consisting of a single node
# with identifier 0, keys 0 - 255

def main():
	start_node = ChordNode(0)
	start_node.start()


if __name__ == '__main__':
    main()

