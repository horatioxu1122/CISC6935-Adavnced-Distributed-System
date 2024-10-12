import socket
import time
import threading

def start_client(ip_port=('127.0.0.1', 9999)):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(ip_port)
    print(f'Connected to server at {ip_port}, waiting for Pi calculation...')
    server_reply = client_socket.recv(1024).decode()
    print(f'Server reply: {server_reply}')
    client_socket.close()

if __name__ == '__main__':
    for i in range(20): # launch 20 computing jobs with 10-second intervals
        print(f'Starting client job {i+1}')
        threading.Thread(target=start_client).start()
        time.sleep(10)  # 10-second interval between jobs
