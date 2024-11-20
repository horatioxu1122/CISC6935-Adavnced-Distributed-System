import threading
import socket
import json
from message_util import send_message, receive_message, send_request
import random
import time


class RaftNode:
    def __init__(self, name, host, port, peers):
        self.name = name
        self.host = host
        self.port = port
        self.peers = peers
        self.current_term = 0
        self.voted_for = None
        self.is_leader = False
        self.state_machine = {}
        self.log = []
        self.commit_index = -1
        self.last_applied = -1
        self.votes_received = 0
        self.leader_address = None  # Initialize leader_address as None
        self.last_heartbeat_time = 0  # Track the time of the last received heartbeat
    
    def append_to_log(self, term, index, command, key, value=None):
        log_entry = {
            "term": term,
            "index": index,
            "command": command,
            "key": key,
            "value": value
        }
        self.log.append(log_entry)
        print(f"{self.name}: Log appended - {log_entry}")

        # Persist log to a file for each node
        log_file = f"{self.name}_log.txt"
        with open(log_file, "a") as f:
            f.write(json.dumps(log_entry) + "\n")

    def start(self):
        print(f"{self.name} starting on {self.host}:{self.port}")
        threading.Thread(target=self.listen_for_messages, daemon=True).start()
        self.start_election_timer()

    def start_election_timer(self):
        if hasattr(self, 'election_timer') and self.election_timer:
            self.election_timer.cancel()
        timeout = random.uniform(5, 10)  # Adjustable timeout range
        self.heartbeat_received = False  # Reset the heartbeat flag on each timer start
        self.election_timer = threading.Timer(timeout, self.check_heartbeat_and_start_election)
        self.election_timer.start()

    def check_heartbeat_and_start_election(self):
        current_time = time.time()
        # If no heartbeat was received recently, start a new election
        if not self.heartbeat_received or (current_time - self.last_heartbeat_time > 10):  # Example timeout
            print(f"{self.name}: No heartbeat received, starting a new election.")
            self.start_election()
        else:
            print(f"{self.name}: Heartbeat received in time, no need for election.")


    def start_election(self, client_triggered=False):
        if not client_triggered:
            if self.is_leader:
                print(f"{self.name}: Already a leader. Election not started.")
                return
            if self.leader_address and self.heartbeat_received:
                # If heartbeat was received recently, do not start a new election
                print(f"{self.name}: A leader is already acknowledged at {self.leader_address}. Election not started.")
                return

        if hasattr(self, 'election_timer'):
            self.election_timer.cancel()  # Cancel the current election timer

        # Reset leader information since we are starting an election
        self.leader_address = None

        self.current_term += 1
        self.voted_for = self.name
        self.votes_received = 1  # Vote for self
        print(f"{self.name}: Starting election for term {self.current_term} {'(Client-triggered)' if client_triggered else ''}")

        # Request votes from peers
        for peer in self.peers:
            threading.Thread(target=self.request_vote, args=(peer,)).start()

        # Start a timer to detect if no leader is elected
        def check_election_result():
            if self.is_leader or self.votes_received > len(self.peers) // 2:
                return  # A leader was elected, or this node became the leader
            print(f"{self.name}: Election for term {self.current_term} failed. Retrying...")
            self.start_election()  # Start a new election

        # Randomize timeout to reduce split-vote likelihood
        timeout = random.uniform(3, 6)
        threading.Timer(timeout, check_election_result).start()


    def request_vote(self, peer):
        import random
        import time
        time.sleep(random.uniform(0.1, 0.5))  # Simulate network latency
        message = {"type": "request_vote", "term": self.current_term, "candidate_id": self.name}
        try:
            response = send_request(peer[0], peer[1], message)
            if response and response.get("vote_granted"):
                self.votes_received += 1
                if self.votes_received > len(self.peers) // 2:
                    self.become_leader()
        except Exception as e:
            print(f"{self.name}: Failed to request vote from {peer} - {e}")

    def become_leader(self):
        self.is_leader = True
        self.election_timer.cancel()  # Cancel election timer as the node is now the leader
        print(f"{self.name}: Became leader for term {self.current_term}")
        self.start_heartbeat()
        
        # Send existing logs to all followers
        for peer in self.peers:
            threading.Thread(target=self.send_append_entries, args=(peer,)).start()

    def start_heartbeat(self):
        if not self.is_leader:
            return
        for peer in self.peers:
            threading.Thread(target=self.send_append_entries, args=(peer,)).start()
        threading.Timer(2, self.start_heartbeat).start()

    def send_append_entries(self, peer):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(peer)
                message = {
                    "type": "append_entries",
                    "term": self.current_term,
                    "leader_id": self.name,
                    "leader_host": self.host,
                    "leader_port": self.port,
                    "leader_commit": self.commit_index,
                    "entries": self.log  # Send entire log or only new entries
                }
                send_message(s, message)
        except Exception as e:
            # print(f"{self.name}: Failed to send AppendEntries to {peer} - {e}")
            return None

    def listen_for_messages(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind((self.host, self.port))
            server.listen()
            print(f"{self.name}: Listening on {self.host}:{self.port}")
            while True:
                conn, _ = server.accept()
                threading.Thread(target=self.handle_message, args=(conn,)).start()

    def handle_message(self, conn):
        try:
            message = receive_message(conn)
            if not message:
                print(f"{self.name}: Received an invalid or empty message.")
                return  # Ignore and return early
            
            if "type" not in message:
                print(f"{self.name}: Malformed message received: {message}")
                return  # Ignore and return early

            if message["type"] == "request_vote":
                self.handle_vote_request(conn, message)
            elif message["type"] == "append_entries":
                self.handle_append_entries(conn, message)
            elif message["type"] == "client_command":
                self.handle_client_command(conn, message)
            else:
                print(f"{self.name}: Unknown message type received: {message}")
        except Exception as e:
            print(f"{self.name}: Error handling message - {e}")
        finally:
            conn.close()

    def handle_vote_request(self, conn, message):
        term = message["term"]
        candidate_id = message["candidate_id"]

        if term > self.current_term:
            # Candidate's term is newer; grant vote
            self.current_term = term
            self.voted_for = candidate_id
            self.is_leader = False  # Step down if this node was a leader
            self.leader_address = None  # Clear outdated leader address
            self.start_election_timer()  # Reset election timer
            send_message(conn, {"vote_granted": True})
            print(f"{self.name}: Voted for {candidate_id} in term {term}")
        elif term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
            # Reaffirm vote for the same candidate in the current term
            self.voted_for = candidate_id
            send_message(conn, {"vote_granted": True})
            print(f"{self.name}: Reaffirmed vote for {candidate_id} in term {term}")
        else:
            # Reject the vote if term is outdated or already voted for another
            send_message(conn, {"vote_granted": False})
            print(f"{self.name}: Rejected vote request from {candidate_id} in term {term}")

    def handle_append_entries(self, conn, message):
        term = message["term"]
        leader_host = message["leader_host"]
        leader_port = message["leader_port"]
        leader_commit = message.get("leader_commit", -1)
        entries = message.get("entries", [])

        current_time = time.time()
        if term >= self.current_term:
            # Acknowledge new leader
            self.current_term = term
            self.voted_for = None
            self.is_leader = False
            self.leader_address = (leader_host, leader_port)
            self.heartbeat_received = True
            self.last_heartbeat_time = current_time  # Update heartbeat time

            # Reset election timer because a valid leader is acknowledged
            self.start_election_timer()  
            print(f"{self.name}: Received heartbeat from {self.leader_address} at term {term}")

            # Append entries from leader to log if they are new
            for entry in entries:
                if entry["index"] >= len(self.log):
                    self.log.append(entry)
                    print(f"{self.name}: Appended log entry from leader: {entry}")

            # Update commit index and apply logs to state machine
            self.commit_index = min(leader_commit, len(self.log) - 1)
            self.apply_committed_entries()

            # Persist updated log to disk
            self.persist_log()

            send_message(conn, {"success": True})
        else:
            send_message(conn, {"success": False})


    def handle_client_command(self, conn, message):
        if message["command"].upper() == "TIMEOUT":
            print(f"{self.name}: Client triggered election timeout.")
            self.start_election(client_triggered=True)
            send_message(conn, {"success": True, "message": "Election triggered."})
            return

        if not self.is_leader:
            print(f"{self.name}: Redirecting client to leader at {self.find_leader_address()}")
            send_message(conn, {"redirect": self.find_leader_address()})
            return

        command_parts = message["command"].split()
        command = command_parts[0].upper()
        key = command_parts[1]
        value = command_parts[2] if len(command_parts) > 2 else None

        if command == "SET":
            self.state_machine[key] = value
            self.append_to_log(self.current_term, len(self.log), "SET", key, value)
            send_message(conn, {"success": True, "message": "Command applied."})
        elif command == "DELETE":
            if key in self.state_machine:
                del self.state_machine[key]
                self.append_to_log(self.current_term, len(self.log), "DELETE", key)
                send_message(conn, {"success": True, "message": f"Deleted key {key}"})
            else:
                send_message(conn, {"success": False, "message": f"Key {key} not found"})
        elif command == "GET":
            value = self.state_machine.get(key)
            print(f"{self.name}: GET {key} -> {value}")
            send_message(conn, {"success": True, "value": value})
        else:
            send_message(conn, {"success": False, "message": "Unknown command."})

    def find_leader_address(self):
        if self.leader_address:
            return {"host": self.leader_address[0], "port": self.leader_address[1]}
        # Fallback to the first peer if no leader is known
        return {"host": self.peers[0][0], "port": self.peers[0][1]}

    def persist_log(self):
        log_file = f"{self.name}_log.txt"
        with open(log_file, "w") as f:
            for entry in self.log:
                f.write(json.dumps(entry) + "\n")
        # print(f"{self.name}: Persisted log to {log_file}")


    def apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            command = entry["command"]
            key = entry["key"]
            value = entry["value"]

            if command == "SET":
                self.state_machine[key] = value
            elif command == "DELETE" and key in self.state_machine:
                del self.state_machine[key]

            print(f"{self.name}: Applied log entry to state machine: {entry}")
