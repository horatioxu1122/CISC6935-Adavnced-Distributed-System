
import socket
import json
from monitor_lib_ex import get_cpu_status
from monitor_lib_ex import get_memory_status
import pickle
from math import acos   # Python3 program to calculate the value of pi up to 3 decimal places


class RPCHandler:
    def __init__(self):
        self._functions = { }

    def register_function(self, func):
        self._functions[func.__name__] = func

    def handle_connection(self, connection):
        try:
            while True:
                # Receive a message
                func_name, args, kwargs = pickle.loads(connection.recv())
                # Run the RPC and send a response
                try:
                    r = self._functions[func_name](*args,**kwargs)
                    connection.send(pickle.dumps(r))
                except Exception as e:
                    connection.send(pickle.dumps(e))
        except EOFError:
             pass



from multiprocessing.connection import Listener
from threading import Thread

def rpc_server(handler, address, authkey):
    sock = Listener(address, authkey=authkey)
    while True:
        client = sock.accept()
        t = Thread(target=handler.handle_connection, args=(client,))
        t.daemon = True
        t.start()
#        break


# Some remote functions
#We calculate pi using the Leibniz's formula
#This series is never-ending, the more the terms this series contains, 
#the closer the value of X converges to Pi value.
def calculate_pi(number_of_iter):   
    # Initialize denominator
    k = 1
 
    # Initialize sum
    s = 0
 
    for i in range(number_of_iter):
        # even index elements are positive
        if i % 2 == 0:
            s = s + 4/k
        else:
        # odd index elements are negative
            s -= 4/k
        # denominator is odd
        k += 2
    print(s)
    return s 

# Function that returns the CPU usage
def get_cpu_usage():
    cpu_status = get_cpu_status()
    return cpu_status

# Function that returns the mem usage
def get_mem_usage():
    mem_usage = get_memory_status()
    # convert dictionary into string
   # mem = json.dumps(mem_usage)
    return mem_usage

#RPC started
def start_rpc_connection(client_id):
    #Register with a handler
    handler = RPCHandler()
    handler.register_function(calculate_pi)
    handler.register_function(get_cpu_usage)
    handler.register_function(get_mem_usage)
    rpc_server(handler, ('localhost', client_id + 1), authkey=b'peekaboo')   # Run the server

#Socket programming starts here...
ip_port = ('127.0.0.1', 9999)

s = socket.socket()
s.connect(ip_port)

count = 0
while True:
    cpu_status = get_cpu_status()
    mem_usage = get_memory_status()
    
    print(cpu_status)
    print("___________")
    print(mem_usage)

    # convert dictionary into string  using json.dumps()
    cpu = json.dumps(cpu_status)
    mem = json.dumps(mem_usage)
    mem = mem[0:650]                #Length was too long. So I shortened it here to get the first 650 chars
    s.sendall(cpu.encode());
    server_reply = s.recv(1024).decode()
    print(server_reply)

    s.sendall(mem.encode())
    server_reply = s.recv(1024).decode()
    print(server_reply) 
    
    client_id = server_reply.partition(' ')[0]
    print('client_id '+ client_id)

    count = count+1
    if count == 2:
        #RPC started
        #Register with a handler
        handler = RPCHandler()
        handler.register_function(calculate_pi)
        handler.register_function(get_cpu_usage)
        handler.register_function(get_mem_usage)
        s.sendall('exit'.encode())
        rpc_server(handler, ('localhost', int(client_id)+1), authkey=b'peekaboo')   # Run the server
        break
s.close()


