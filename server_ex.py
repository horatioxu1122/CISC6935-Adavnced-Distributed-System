import socket
import threading
import pickle
from multiprocessing.connection import Client
import time
import json
import operator

class RPCProxy:
    def __init__(self, connection):
        self._connection = connection
    def __getattr__(self, name):
        def do_rpc(*args, **kwargs):
            self._connection.send(pickle.dumps((name, args, kwargs)))
            result = pickle.loads(self._connection.recv())
            if isinstance(result, Exception):
                raise result
            return result
        return do_rpc

idle_workers = []
workers = []

def add_to_current_workers(client_id):
    if client_id not in workers:
         workers.append(client_id)

 #   for j in workers:
 #       print('Workers test ' + j)

def detect_idle_workers(client_id):
    if client_id not in workers:
        workers.append(client_id)


#Socket programming starts here

def link_handler(link, client):
    print('server start to receiving msg from [%s:%s]....' % (client[0], client[1]))
    while True:
        add_to_current_workers(client[1])
        client_data = link.recv(1024).decode()
        
        if client_data == "exit":
            print('communication end with [%s:%s]...' % (client[0], client[1]))
            break
        print('client from [%s:%s] send a msg：%s' % (client[0], client[1], client_data))
        link.sendall('%s server had received your msg'.encode() % (str(client[1]).encode()))
    link.close()

#The round robin algorithm creates a queue and then puts all the workers inside the queue. Then assigns the work
#in the order in which the workers were added to the queue and it continues to pop from the queue whenever a client is called. It then appends the popped worker to the back of the queue. The loop continues until the job is done
def round_robin_algorithm(workers):
    # Initializing a queue
    queue = []
  
    # Adding elements to the queue
    for i in workers:
        queue.append(i)
  
        print("Initial queue")
        print(queue)

    j = 0
    while j < 5:                    #for 4 workers, each iteration will submit 4 jobs, so 5 * 4 = 20 jobs
        time.sleep(10)               #interval of 1second
        for i in queue:
            c = Client(('localhost', i+1), authkey=b'peekaboo')
            proxy = RPCProxy(c)
            print(str(i) + ' : ' + str(proxy.calculate_pi(1000000)))  #Number of terms
            workers_cpu_usage[i] = proxy.get_cpu_usage()
            workers_mem_usage[i] = proxy.get_mem_usage()
        
            currently_used_worker = queue.pop()
            queue.append(currently_used_worker)    #move to the back of the queue
        
        #Just for testing purposes
        print('printing the cpu usage after RR job submissions:')
        for s in workers_cpu_usage:
            print(str(s) + ' : '+ str(workers_cpu_usage[s]))

        j = j+1


ip_port = ('127.0.0.1', 9999)

sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # socket.SOCK_STREAM is tcp
sk.bind(ip_port)
sk.listen(5)

print('start socket server，waiting client...')

while True:
    conn, address = sk.accept()
    print('create a new thread to receive msg from [%s:%s]' % (address[0], address[1]))
    t = threading.Thread(target=link_handler, args=(conn, address))   
    t.start()

    #Rpc
    time.sleep(2)  #sleep for 2 seconds

    workers_cpu_usage = {}
    workers_mem_usage = {}

    #Dyanamically connects to the individually created client RPCs and gets the memory and cpu for each of them.
    #It also makes them calculate pie one-time each
    print('Calling each worker one time and making it calculate pie --testing')
    for i in workers:
        c = Client(('localhost', i+1), authkey=b'peekaboo')
        proxy = RPCProxy(c)
        print(str(i) + ' : ' + str(proxy.calculate_pi(3600000)))  #Number of terms
        workers_cpu_usage[i] = proxy.get_cpu_usage()
        workers_mem_usage[i] = proxy.get_mem_usage()

    #printing the cpu for testing purposes
    print('printing the cpu for testing purposes')
    for s in workers_cpu_usage:
        print(str(s) + ' : '+ str(workers_cpu_usage[s]))
    
    #printing the memory for testing purposes
    print('printing the memory for testing purposes')
    for s in workers_mem_usage:
        print(str(s) + ' : '+ str(workers_mem_usage[s]))

    print('#####################################################################################')

    print('idle worker implementation starts here:')
    #To make sure all previous CPU data are cleared from all clients before running the next loop
    # time.sleep(15)    #Will reset all CPUs. Very good for testing our idle_workers list. Uncomment the line to see it
    #From Here we put in the code that allows the manager to detect idling workers
    workers_cpu_usage_processed = {}
    idle_workers = []    #The manager can detect idling workers
    for s in workers_cpu_usage:
        #for each of the client cpu data saved, we get the key with the maximum value saved
        max_key =  max(workers_cpu_usage[s].items(), key=operator.itemgetter(1))[0]
        workers_cpu_usage_processed[s] = workers_cpu_usage[s][max_key]
        
        #if the maximum cpu usage accross all the levels (Lavg_1, lavg_2 and lavb_3) is zero, it means the worker is idle
        if workers_cpu_usage[s][max_key] == '0.00':
            idle_workers.append(s)

    #printing the idle worker for testing purposes
    print('printing the idle worker for testing purposes')
    for s in idle_workers:
        print(str(s) + ' : ' + str(s))
    print('idle workers DONE printing')

    print('#####################################################################################')

    #Manager assigning work based on workers resources.
    #Here we simply show the manager looping through the idle_worker list and assigning any client found it work to do
    print('Manager assigning work based on workers resources')
    for i in workers:
        if i in idle_workers:
            c = Client(('localhost', i+1), authkey=b'peekaboo')
            proxy = RPCProxy(c)
            print(str(i) + ' : ' + str(proxy.calculate_pi(100000)))  #Number of terms
            workers_cpu_usage[i] = proxy.get_cpu_usage()
            workers_mem_usage[i] = proxy.get_mem_usage()
    
    #printing the cpu for testing purposes
    print('printing the cpu for testing purposes')
    for s in workers_cpu_usage:
        print(str(s) + ' : '+ str(workers_cpu_usage[s]))
    
    print('#####################################################################################')

    print('Printng the data in our workers_cpu_usage_processed dictionary:')
    for s in workers_cpu_usage_processed:
        print(str(s) + ' : '+ str(workers_cpu_usage_processed[s]))

    print('Our load balancing algorithm starts here:')
    #Load balancing algorithm.
    #Always select the one with the lowest CPU usage
    sorted_dict = dict(sorted(workers_cpu_usage_processed.items(), key=lambda x: x[1]))
    for j, k in sorted_dict.items():
        if j in workers:
            c = Client(('localhost', j+1), authkey=b'peekaboo')
            proxy = RPCProxy(c)
            print(str(j) + ' : ' + str(proxy.calculate_pi(10000000)))  #Number of terms
            workers_cpu_usage[j] = proxy.get_cpu_usage()
            workers_mem_usage[j] = proxy.get_mem_usage()


    #printing the cpu for testing purposes
    print('printing the cpu for testing purposes to observe the order of assignment:')
    for s in workers_cpu_usage:
        print(str(s) + ' : '+ str(workers_cpu_usage[s]))

    print('#####################################################################################')
    print('20 jobs at 10 intervals submitted regularly to be compared  with the round robin algo: ')
    i = 0
    while i < 5:              # 4 workers for each round, for 5 times == 5*4 = 20 jobs
        for j in workers:
            time.sleep(10)    # Interval  #will be made 10 seconds per the requirements
            c = Client(('localhost', j+1), authkey=b'peekaboo')
            proxy = RPCProxy(c)
            print(str(j) + ' : ' + str(proxy.calculate_pi(1000000)))  #Number of terms
            workers_cpu_usage[j] = proxy.get_cpu_usage()
            workers_mem_usage[j] = proxy.get_mem_usage()

        #for testiing purposes
        print('printing cpu usage after regular job submission:')
        for s in workers_cpu_usage:
            print(str(s) + ' : '+ str(workers_cpu_usage[s]))
        i = i + 1

    print('#####################################################################################')
    
    #Calling the round robin function to execute our round robin algorithm
    print('Round Robin Algorithm starts here')
    round_robin_algorithm(workers)
