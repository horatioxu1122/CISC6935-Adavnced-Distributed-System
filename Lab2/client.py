import json
import socket
from message_util import send_request

def send_command_to_leader(command, nodes):
    """Send a command to the cluster."""
    for node in nodes:
        host, port = node
        try:
            response = send_request(host, port, {"type": "client_command", "command": command})
            if response.get("redirect"):
                leader = response["redirect"]
                print(f"Redirected to leader at {leader['host']}:{leader['port']}")
                return send_request(leader["host"], leader["port"], {"type": "client_command", "command": command})
            return response
        except Exception as e:
            print(f"Failed to connect to {host}:{port} - {e}")
    return {"error": "All nodes are unreachable."}

if __name__ == "__main__":
    # Load cluster nodes from configuration
    nodes = [
        ("10.128.0.10", 5000),
        ("10.128.0.11", 5001),
        ("10.128.0.13", 5002)
    ]

    print("Interactive client. Type commands (SET, GET, DELETE, TIMEOUT, EXIT):")

    while True:
        command = input("Enter command: ").strip()
        if command.upper() == "EXIT":
            print("Exiting client.")
            break

        print(f"Sending command: {command}")
        response = send_command_to_leader(command, nodes)
        print("Response:", response)
