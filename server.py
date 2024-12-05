import json
import sys
import threading
from raft_node import RaftNode
from message_util import send_request
import time
import uuid

def handle_transaction(transaction, coordinator, participants):
    transaction_id = str(uuid.uuid4())  # Unique transaction identifier
    transaction["transaction_id"] = transaction_id

    print(f"Coordinator: Sending PREPARE to all participants for transaction {transaction}")
    prepare_success = send_prepare(transaction, participants)

    if not prepare_success:
        print(f"Coordinator: PREPARE failed. Sending ROLLBACK.")
        send_rollback(transaction, participants)
        return {"status": "failure", "message": "Transaction aborted during PREPARE phase."}

    print("Coordinator: All participants agreed. Pausing before COMMIT for testing...")
    time.sleep(2)

    print(f"Coordinator: All participants agreed. Sending COMMIT.")
    send_commit(transaction, participants)
    return {"status": "success", "message": "Transaction committed successfully."}

def send_prepare(transaction, participants):
    responses = []
    for participant in participants:
        response = send_request(participant[0], participant[1], {
            "type": "PREPARE",
            "transaction": transaction
        })
        if response and response.get("status") == "YES":
            responses.append(True)
        else:
            responses.append(False)
    return all(responses)

def send_commit(transaction, participants):
    for participant in participants:
        response = send_request(participant[0], participant[1], {
            "type": "COMMIT",
            "transaction": transaction
        })
        if response and response.get("status") == "ALREADY_COMMITTED":
            print(f"Commit already applied on {participant[0]}:{participant[1]}, skipping.")

def send_rollback(transaction, participants):
    for participant in participants:
        send_request(participant[0], participant[1], {
            "type": "ROLLBACK",
            "transaction": transaction
        })

def transaction_listener(peers, coordinator_node):
    try:
        print("Transaction listener thread started.")
        ongoing_transaction = threading.Event()  # Use an event to track transaction status

        def handle_transaction_thread(transaction, operation):
            """Threaded function to handle a transaction."""
            ongoing_transaction.set()  # Mark transaction as ongoing
            result = handle_transaction(operation, coordinator_node, peers)
            print(f"Transaction result: {result}")
            ongoing_transaction.clear()  # Mark transaction as completed

        while True:
            print("Enter transaction (TRANSFER <amount>, BONUS <percentage>, or a crash command):")
            transaction = input().strip()

            if transaction.upper() == "EXIT":
                print("Exiting...")
                break

            if transaction.upper() == "CRASH_NODE2_BEFORE":
                print("Simulating Node 2 crash before responding to PREPARE...")
                send_request(peers[0][0], peers[0][1], {"type": "SIMULATE_CRASH", "phase": "before"})
                continue

            if transaction.upper() == "CRASH_NODE2_AFTER":
                print("Simulating Node 2 crash after responding to PREPARE...")
                send_request(peers[0][0], peers[0][1], {"type": "SIMULATE_CRASH", "phase": "after"})
                continue

            if transaction.upper() == "CRASH_COORDINATOR":
                print("Simulating Coordinator crash...")
                coordinator_node.simulate_crash(duration=15)  # Increase crash duration for testing
                continue

            # Check if a transaction is already ongoing
            if ongoing_transaction.is_set():
                print("A transaction is already in progress. Please wait...")
                continue

            # Split and validate the input command
            parts = transaction.split()
            if len(parts) != 2:
                print("Invalid command format. Use 'TRANSFER <amount>' or 'BONUS <percentage>'.")
                continue

            command, value = parts
            if command.upper() == "TRANSFER" and value.isdigit():
                operation = {"type": "TRANSFER", "amount": int(value)}
            elif command.upper() == "BONUS" and value.replace('.', '', 1).isdigit():
                percentage = float(value)

                # Get balance from Node 2
                node2_balance_response = send_request(peers[0][0], peers[0][1], {"type": "GET", "key": "balance"})
                if not node2_balance_response or not node2_balance_response.get("success"):
                    print("Failed to retrieve balance for Node 2. Retrying...")
                    continue

                node2_balance = node2_balance_response.get("value", 0)
                bonus_amount = (percentage / 100) * node2_balance
                operation = {"type": "BONUS", "amount": bonus_amount}
            else:
                print("Invalid command. Use 'TRANSFER <amount>' or 'BONUS <percentage>'.")
                continue

            # Start transaction in a separate thread
            transaction_thread = threading.Thread(target=handle_transaction_thread, args=(transaction, operation))
            transaction_thread.daemon = True
            transaction_thread.start()

    except Exception as e:
        print(f"Error in transaction listener: {e}")

if __name__ == "__main__":
    try:
        # Load cluster configuration from cluster_config.json
        with open("cluster_config.json") as f:
            config = json.load(f)
        
        # Extract node information from command-line argument
        node_name = sys.argv[1]
        node_config = next(node for node in config["nodes"] if node["name"] == node_name)

        # Define peers for each node including replicas (specific for nodes with replicas)
        if node_name == "node2":
            peers = [
                ("127.0.0.1", 5003),  # Replica 1 for Account A
                ("127.0.0.1", 5004)   # Replica 2 for Account A
            ]
        elif node_name == "node3":
            peers = [
                ("127.0.0.1", 5005),  # Replica 1 for Account B
                ("127.0.0.1", 5006)   # Replica 2 for Account B
            ]
        else:
            # For other nodes (node1 or replicas), determine peers differently
            peers = [(peer["host"], peer["port"]) for peer in config["nodes"] if peer["name"] != node_name]

        # Initialize the RaftNode
        node = RaftNode(node_name, node_config["host"], node_config["port"], peers, f"{node_name}_account.json")
        node.start()

        # Start listener if this is the coordinator (node1)
        if node_name == "node1":
            print("Coordinator node started. Waiting for transactions...")
            listener_thread = threading.Thread(target=transaction_listener, args=(peers, node), daemon=True)
            listener_thread.start()
            listener_thread.join()  # Wait for the thread to complete

        # Keep the main thread alive
        while True:
            pass

    except Exception as e:
        print(f"Error in main: {e}")