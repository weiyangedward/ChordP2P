"""
Read config file:
1. only two rows in config
2. the second row has the IP and PORT
3. all later added node at Chord use PORT++
"""

def read_config(para):
    with open('config', 'r') as f:
        min_delay, max_delay = 0, 0
        starting_port = 0
        for i, line in enumerate(f):
            args = line.split()
            if i == 0:
                min_delay, max_delay = int(args[0]), int(args[1])
            else:
                starting_port = int(args[0])

        if para == 'starting_port':
            return starting_port
        elif para == 'delay':
            return (min_delay/1000.0, max_delay/1000.0)


def get_starting_port():
    return read_config('starting_port')


def get_delay_info():
    return read_config('delay')