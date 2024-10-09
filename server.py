import socket
import threading
import json
from monitor_lib_ex import get_cpu_status, get_memory_status
from math import pi

# Function to calculate Pi using the Leibniz formula
def calculate_pi(terms):
    k = 1
    pi_sum = 0
    for i in range(terms):
        if i % 2 == 0:
            pi_sum += 4/k
        else:
            pi_sum -= 4/k
        k += 2
    return pi_sum

# Function to handle each client connection
def link_handler(conn, client):
    print(f'Server connected to [{client[0]}:{client[1]}]...')
    while True:
        client_data = conn.recv(1024).decode()
        if client_data == "exit":
            print(f'Communication ended with [{client[0]}:{client[1]}].')
            break

        if client_data.startswith("cpu"):
            cpu_status = json.dumps(get_cpu_status())
            conn.sendall(cpu_status.encode())
        elif client_data.startswith("mem"):
            mem_status = json.dumps(get_memory_status())
            conn.sendall(mem_status.encode())
        elif client_data.startswith("pi"):
            try:
                terms = int(client_data.split(" ")[1])  # Extract the number of terms for Pi calculation
                pi_value = calculate_pi(terms)
                conn.sendall(f"Pi calculated: {pi_value}".encode())
            except (IndexError, ValueError):
                conn.sendall("Invalid input for Pi calculation. Use format: pi <number_of_terms>".encode())
        else:
            print(f'Message from [{client[0]}:{client[1]}]: {client_data}')
            conn.sendall('Server received your message.'.encode())
    
    conn.close()

# Server setup
ip_port = ('127.0.0.1', 9999)
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(ip_port)
server_socket.listen(5)

print('Server started, waiting for connections...')

# Main loop to accept incoming client connections
while True:
    conn, address = server_socket.accept()
    print(f'New connection from [{address[0]}:{address[1]}].')
    thread = threading.Thread(target=link_handler, args=(conn, address))
    thread.start()
