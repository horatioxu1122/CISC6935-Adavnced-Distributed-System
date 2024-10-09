import socket

ip_port = ('127.0.0.1', 9999)
client_socket = socket.socket()
client_socket.connect(ip_port)

while True:
    inp = input('Input message (cpu/mem/pi <terms>/exit): ').strip()  # Input "pi <number_of_terms>" to calculate Pi
    if not inp:
        continue

    client_socket.sendall(inp.encode())  # Send input to server

    if inp == "exit":
        print("Communication ended.")
        break

    server_reply = client_socket.recv(1024).decode()  # Receive server's response
    print(f"Server reply: {server_reply}")

client_socket.close()
