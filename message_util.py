import socket
import json

def send_message(conn, message):
    if isinstance(message, dict):
        message = json.dumps(message)
    message_bytes = message.encode('utf-8')
    header = f"{len(message_bytes):<10}".encode('utf-8')
    conn.sendall(header + message_bytes)

def receive_message(conn):
    try:
        length_header = conn.recv(10).decode('utf-8').strip()
        if not length_header:
            raise ValueError("Connection closed by peer.")
        if not length_header.isdigit():
            raise ValueError(f"Invalid length header received: '{length_header}'")
        message_length = int(length_header)
        message_bytes = b""
        while len(message_bytes) < message_length:
            chunk = conn.recv(message_length - len(message_bytes))
            if not chunk:
                raise ValueError("Incomplete message body received.")
            message_bytes += chunk
        message = message_bytes.decode('utf-8')
        return json.loads(message)
    except Exception as e:
        print(f"Error in receive_message: {e}")
        return None

def send_request(host, port, message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as conn:
            conn.connect((host, port))
            send_message(conn, message)
            response = receive_message(conn)
        return response
    except (ConnectionRefusedError, ConnectionResetError) as e:
        # print(f"Error in send_request to {host}:{port} - {e}")
        return None
    except Exception as e:
        print(f"Unexpected error in send_request: {e}")
        return None
