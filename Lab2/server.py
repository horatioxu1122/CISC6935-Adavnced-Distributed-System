import json
import sys
from raft_node import RaftNode

if __name__ == "__main__":
    with open("cluster_config.json") as f:
        config = json.load(f)

    node_name = sys.argv[1]
    node_config = next(node for node in config["nodes"] if node["name"] == node_name)
    peers = [(peer["host"], peer["port"]) for peer in config["nodes"] if peer["name"] != node_name]

    node = RaftNode(node_name, node_config["host"], node_config["port"], peers)
    node.start()

    # Keep the main thread alive
    while True:
        pass
