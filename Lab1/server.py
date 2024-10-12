import socket
import threading
import queue
import time
import psutil  # For CPU and memory monitoring
from monitor_lib import get_cpu_status, get_memory_status
import os

POOL_SIZE = 4
ROUND_ROBIN_INDEX = 0

connection_queue = queue.Queue() # shared queue for client connections
worker_status = {}
worker_cpu_usage = {}
lock = threading.Lock()
PI_ITERATIONS = 36000000

# def get_worker_cpu_usage():
#     return get_cpu_status()

# not used directly for load balancing but still monitored
def get_mem_usage():
    return get_memory_status()

def calculate_pi(number_of_iter):
    k = 1
    s = 0
    for i in range(number_of_iter):
        if i % 2 == 0:
            s = s + 4 / k
        else:
            s = s - 4 / k
        k += 2
    return s

# gets client connection from the queue and processes it
def worker_function(worker_id):
    global worker_status
    def monitor_worker():
        process = psutil.Process(os.getpid())  # create Process object for this particular worker
        while True:
            time.sleep(10)
            cpu_percent = process.cpu_percent(interval=1)  # get CPU percent for this worker
            with lock:
                worker_cpu_usage[worker_id] = cpu_percent  # store the worker's CPU usage
            mem_info = get_mem_usage()
            print(f'Worker {worker_id} - CPU Percent: {cpu_percent}%')
            print(f'Worker {worker_id} - Memory Info: {mem_info}')
    monitoring_thread = threading.Thread(target=monitor_worker, daemon=True) # start the monitoring thread for this worker
    monitoring_thread.start()
    while True:
        conn, client, algorithm = connection_queue.get()
        if conn is None:
            break
        start_time = time.time()
        with lock:
            worker_status[worker_id] = 'busy'
        try:
            print(f'Worker {worker_id} starting Pi calculation for {PI_ITERATIONS} iterations...')
            pi_value = calculate_pi(PI_ITERATIONS)
            conn.sendall(f'Pi calculated: {pi_value}'.encode()) # Send the result to the client
            conn.recv(1024)
        except ConnectionResetError:
            print(f'Connection lost with [{client[0]}:{client[1]}]')
        finally:
            conn.close()
            end_time = time.time()
            elapsed_time = end_time - start_time 
            print(f'Worker {worker_id} finished job in {elapsed_time:.2f} seconds using {algorithm} algorithm.')
            with lock:
                worker_status[worker_id] = 'idle'
            connection_queue.task_done()

# manager function to monitor worker status every 10 seconds
def manager():
    while True:
        time.sleep(10)  # check every 10 seconds
        with lock:
            for worker_id, status in worker_status.items():
                print(f'Worker [{worker_id}] is {status}.')
                print("----------------")

# assign a client connection to the worker with the lowest CPU usage
def assign_by_cpu(conn, client_address):
    with lock:
        least_cpu_worker = min(worker_cpu_usage, key=worker_cpu_usage.get) # find the worker with the lowest CPU usage
        print(f'Assigning connection from {client_address} to worker {least_cpu_worker} (CPU-based)')
    connection_queue.put((conn, client_address, 'CPU-based')) # Put the client connection into the queue for the selected worker

# assign a client connection using round-robin scheduling
def assign_by_round_robin(conn, client_address):
    global ROUND_ROBIN_INDEX
    with lock:
        worker_id = ROUND_ROBIN_INDEX
        ROUND_ROBIN_INDEX = (ROUND_ROBIN_INDEX + 1) % POOL_SIZE  # move to the next worker in round-robin way
        print(f'Assigning connection from {client_address} to worker {worker_id} (Round-robin)')
    connection_queue.put((conn, client_address, 'Round-robin')) # put the client connection into the queue for the selected worker

def start_server(ip_port=('127.0.0.1', 9999)):
    workers = [] # create pool of workers
    for i in range(POOL_SIZE):
        worker_status[i] = 'idle'
        worker_cpu_usage[i] = float('inf')  # initialize CPU usage for each worker
        worker_thread = threading.Thread(target=worker_function, args=(i,))
        worker_thread.daemon = True  # make threads exit when the main thread exits
        workers.append(worker_thread)
        worker_thread.start()
    manager_thread = threading.Thread(target=manager, daemon=True) # start the manager thread to check idle workers every 10 seconds
    manager_thread.start()
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # create server socket to accept connections
    server_socket.bind(ip_port)
    server_socket.listen(5)
    print(f'Server started at {ip_port}, waiting for clients...')
    connection_count = 0
    while True:
        conn, client_address = server_socket.accept() # accept new client connection
        print(f'New connection from [{client_address[0]}:{client_address[1]}]')
        if connection_count % 2 == 0: # alternate between round-robin and CPU-based assignment
            assign_by_cpu(conn, client_address)
        else:
            assign_by_round_robin(conn, client_address)
        connection_count += 1

if __name__ == '__main__':
    start_server()
