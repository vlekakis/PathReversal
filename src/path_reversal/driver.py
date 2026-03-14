"""Driver for the Path Reversal distributed mutual exclusion system.

Orchestrates node processes, executes scenarios, verifies correctness,
and optionally visualizes the pointer graph evolution.
"""

import logging
import argparse
import signal
import zmq
import pickle
import subprocess
from time import sleep
from multiprocessing import Process
from queue import Queue, Empty
from shutil import rmtree
from os import makedirs
from random import choice

from path_reversal.status import NodeStatus, PRStatus
from path_reversal.message import MsgType, MsgFactory
from path_reversal.algorithm import PRStatusUpdate
from path_reversal.node import FifoNode
from path_reversal.sink import AgentSinkServer

logger = logging.getLogger(__name__)

STATUS_NAMES = {
    PRStatus.EATING: "Eating",
    PRStatus.THINKING: "Thinking",
    PRStatus.HUNGRY: "Hungry",
}


class Driver:
    """Orchestrates the Path Reversal simulation."""

    def __init__(self, nodes_file, scenario_file, log_dir='logs',
                 graph_dir='graphProgress', clean=False, manual=False,
                 enable_graphs=False):
        self.nodes = {}
        self.node_processes = []
        self.node_status = {}
        self.log_dir = log_dir
        self.graph_dir = graph_dir
        self.enable_graphs = enable_graphs
        self.nodes_file = nodes_file
        self.scenario_file = scenario_file
        self.manual = manual
        self.graph_counter = 0

        # Graph state (only used if enable_graphs)
        self.graph_last = {}
        self.graph_next = {}

        if clean:
            rmtree(graph_dir, ignore_errors=True)
            makedirs(graph_dir, exist_ok=True)
            rmtree(log_dir, ignore_errors=True)
            makedirs(log_dir, exist_ok=True)
        else:
            makedirs(graph_dir, exist_ok=True)
            makedirs(log_dir, exist_ok=True)

        # Status callback for visualization (set by visualization module)
        self.on_status_update = None

    def _establish_local_node(self, node_name, pub_addr, sink_addr, neighbor):
        fifo_node = FifoNode()
        worker = Process(target=fifo_node.runFifoNetWorker,
                         args=(node_name, pub_addr, sink_addr, neighbor))
        worker.daemon = True
        worker.start()
        logger.info("Node %s initiated", node_name)
        return worker

    def initiate_nodes(self, pub_addr, sink_addr):
        peers = []
        node_counter = 1
        with open(self.nodes_file) as fp:
            for line in fp:
                host = line.strip()
                if not host:
                    continue
                if ':' not in host:
                    logger.error("Invalid node entry: %s", host)
                    raise ValueError(f"Invalid node file entry: {host}")
                if host in self.nodes:
                    logger.warning("Duplicate node %s, skipping", host)
                    continue

                parts = host.split(':')
                counter_str = str(node_counter).zfill(3)
                self.nodes[host] = {
                    'ip': parts[0],
                    'port': parts[1],
                    'status': NodeStatus.DRIVER_PARSED,
                    'name': host,
                    'friendlyName': f"Node{counter_str}",
                }
                node_counter += 1
                peers.append(host)

        for h in peers:
            neighbor = peers[(peers.index(h) + 1) % len(peers)]
            ip = self.nodes[h]['ip']
            if ip in ('localhost', '127.0.0.1') and not self.manual:
                worker = self._establish_local_node(h, pub_addr, sink_addr, neighbor)
                self.node_processes.append(worker)
                self.nodes[h]['status'] = NodeStatus.DRIVER_INITIALIZED
            else:
                logger.warning("Remote node %s not supported", h)

        return peers

    def test_bidirectional(self, sock, in_queue, out_queue):
        checked = 0
        packet = MsgFactory.create(MsgType.AGENT_TEST_MSG)
        for k in self.nodes:
            msg = [self.nodes[k]['name'].encode(), packet]
            sock.send_multipart(msg)

        while True:
            try:
                while not in_queue.empty():
                    msg = in_queue.get(False)
                    node_name = msg[0].decode() if isinstance(msg[0], bytes) else msg[0]
                    self.nodes[node_name]['status'] = NodeStatus.DRIVER_FUNCTIONAL
                    in_queue.task_done()
                    out_queue.put([msg[0], b"ack"])
                    checked += 1
                    if checked == len(self.nodes):
                        logger.info("All %d nodes responded", checked)
                        return
            except Empty:
                continue

    def build_scenario(self):
        scenario = []
        with open(self.scenario_file) as fp:
            for line in fp:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split()
                action = {}
                for part in parts:
                    if part in ('sleep', 'set', 'reset', 'hungry', 'exit'):
                        action['ACTION'] = part
                    else:
                        action['ARG'] = part
                scenario.append(action)
        return scenario

    def update_graph(self, node, last, next_node):
        self.graph_last[node] = [last]
        self.graph_next[node] = [next_node]

    def find_cycles(self):
        """Detect cycles in the 'last' pointer graph."""
        graph = self.graph_last
        no_cycle = []
        todo = set(graph.keys())
        while todo:
            node = todo.pop()
            stack = [node]
            while stack:
                top = stack[-1]
                found_next = False
                for neighbor in graph.get(top, []):
                    if neighbor is None:
                        continue
                    if neighbor in stack:
                        return stack[stack.index(neighbor):]
                    if neighbor in todo:
                        stack.append(neighbor)
                        todo.remove(neighbor)
                        found_next = True
                        break
                if not found_next:
                    stack.pop()
                    no_cycle.append(node)
        return None

    def verify_mutual_exclusion(self, hungry_nodes):
        """Assert exactly one node is EATING and it's in the hungry list."""
        eating_nodes = [n for n, s in self.node_status.items()
                        if s == PRStatus.EATING]
        assert len(eating_nodes) == 1, \
            f"Expected 1 eating node, found {len(eating_nodes)}: {eating_nodes}"
        eating_node = eating_nodes[0]
        assert eating_node in hungry_nodes, \
            f"Eating node {eating_node} not in hungry list {hungry_nodes}"
        hungry_nodes.remove(eating_node)
        logger.info("Mutual exclusion verified: %s is eating", eating_node)

    def verify_operation(self, update, hungry_nodes):
        node = update[0]
        if isinstance(node, bytes):
            node = node.decode()
        data = pickle.loads(update[1])

        old_status = self.node_status.get(node, "N/A")
        self.node_status[node] = data[PRStatusUpdate.STATUS]
        new_status = self.node_status[node]

        logger.info("Node %s: %s -> %s", node,
                     STATUS_NAMES.get(old_status, str(old_status)),
                     STATUS_NAMES.get(new_status, str(new_status)))

        if data[PRStatusUpdate.SATISFY]:
            self.verify_mutual_exclusion(hungry_nodes)

        self.update_graph(node, data[PRStatusUpdate.LAST],
                          data[PRStatusUpdate.NEXT])

        cycle = self.find_cycles()
        if cycle:
            logger.warning("Cycle detected in last pointers: %s", cycle)

        # Notify visualization
        if self.on_status_update:
            self.on_status_update(node, self.node_status.copy(),
                                  self.graph_last.copy())

        self.graph_counter += 1
        status_ack = PRStatusUpdate.createStatusACKMessage(data[PRStatusUpdate.SEQ])
        return (node.encode() if isinstance(node, str) else node, status_ack)

    def play_scenario(self, ctrl_sock, in_queue, out_queue, sink_server):
        hungry_nodes = []
        scenario = self.build_scenario()
        logging.basicConfig(level=logging.INFO,
                            filename=f"{self.log_dir}/objectService.log")

        # Initialize graph from first 'set' command
        for action in scenario:
            if action["ACTION"] == "set":
                root = action["ARG"]
                for n in self.nodes:
                    if n != root:
                        self.update_graph(n, root, None)
                break

        for cmd in scenario:
            # Process any pending status updates
            try:
                update = in_queue.get(True, timeout=1)
                in_queue.task_done()
                node, status_ack = self.verify_operation(update, hungry_nodes)
                out_queue.put([node, status_ack])
            except Empty:
                pass

            if cmd['ACTION'] == 'sleep':
                sleep(int(cmd['ARG']))

            elif cmd['ACTION'] == 'set':
                data_item = str(choice(range(1000)))
                data_id = MsgFactory.generateMessageId(data_item)
                tx_msg = MsgFactory.create(MsgType.PR_SETUP,
                                           dst=cmd['ARG'],
                                           data=data_item,
                                           dataId=data_id)
                logger.info("Setting object %s at node: %s", data_item, cmd['ARG'])
                ctrl_sock.send_multipart([b'Set', tx_msg])

            elif cmd['ACTION'] == 'reset':
                logger.info("Resetting all nodes")
                ctrl_sock.send_string('Reset')

            elif cmd['ACTION'] == 'hungry':
                hungry_nodes.append(cmd['ARG'])
                logger.info("Setting node %s to HUNGRY", cmd['ARG'])
                tx_msg = MsgFactory.create(MsgType.PR_GET_HUNGRY, dst=cmd['ARG'])
                ctrl_sock.send_multipart([cmd['ARG'].encode(), tx_msg])

            elif cmd['ACTION'] == 'exit':
                logger.info("Exiting scenario")
                self._shutdown(ctrl_sock, in_queue, out_queue, sink_server)
                return

    def _shutdown(self, ctrl_sock, in_queue, out_queue, sink_server):
        """Clean shutdown of all processes."""
        logger.info("Sending Exit to all nodes...")
        ctrl_sock.send_string('Exit')
        sleep(1)

        # Join queues with timeout
        sink_server.join(timeout=5)

        for proc in self.node_processes:
            proc.join(timeout=3)
            if proc.is_alive():
                logger.warning("Force terminating node process %s", proc.pid)
                proc.terminate()
                proc.join(timeout=2)

        logger.info("All processes shut down")

    def run(self):
        """Main entry point — set up ZMQ, spawn nodes, run scenario."""
        sink_bind = "tcp://*:9090"
        pub_bind = "tcp://*:5558"
        sink_addr = "tcp://127.0.0.1:9090"
        pub_addr = "tcp://127.0.0.1:5558"

        context = zmq.Context()

        # Set up signal handler for clean shutdown
        original_sigint = signal.getsignal(signal.SIGINT)

        in_queue = Queue(10)
        out_queue = Queue(10)
        sink_sock = context.socket(zmq.REP)
        sink_server = AgentSinkServer(in_queue, out_queue, sink_sock, sink_bind)
        sink_server.start()

        logger.info("Initiating nodes from %s", self.nodes_file)
        peers = self.initiate_nodes(pub_addr, sink_addr)

        ctrl_sock = context.socket(zmq.PUB)
        ctrl_sock.bind(pub_bind)

        logger.info("Waiting for setup to finish...")
        sleep(5)

        logger.info("Testing bidirectional communication...")
        self.test_bidirectional(ctrl_sock, in_queue, out_queue)

        logger.info("Establishing neighbor connections...")
        ctrl_sock.send_string('ConnectToNeighbor')
        sleep(1)

        if len(peers) > 1:
            logger.info("Testing peer-to-peer connections...")
            ctrl_sock.send_string('TestConnectionToNeighbor')

        sleep(5)

        def signal_handler(sig, frame):
            logger.info("Caught SIGINT, shutting down...")
            self._shutdown(ctrl_sock, in_queue, out_queue, sink_server)
            signal.signal(signal.SIGINT, original_sigint)

        signal.signal(signal.SIGINT, signal_handler)

        self.play_scenario(ctrl_sock, in_queue, out_queue, sink_server)


def main():
    p = argparse.ArgumentParser(description='Path Reversal Simulation Driver')
    p.add_argument('-n', dest='nodes', required=True,
                   help='Nodes file (<address:port> per line)')
    p.add_argument('-s', dest='scenario', required=True,
                   help='Scenario file')
    p.add_argument('-l', dest='log_dir', default='logs',
                   help='Log directory (default: logs)')
    p.add_argument('-g', dest='graph_dir', default='graphProgress',
                   help='Graph output directory')
    p.add_argument('-c', dest='clean', action='store_true',
                   help='Clean logs and graphs before running')
    p.add_argument('--manual', action='store_true',
                   help='Manual node management')
    p.add_argument('--graphs', action='store_true',
                   help='Enable graph PNG generation')
    p.add_argument('-v', '--verbose', action='store_true',
                   help='Verbose output')
    args = p.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level,
                        format='%(asctime)s %(name)s %(levelname)s: %(message)s')

    driver = Driver(
        nodes_file=args.nodes,
        scenario_file=args.scenario,
        log_dir=args.log_dir,
        graph_dir=args.graph_dir,
        clean=args.clean,
        manual=args.manual,
        enable_graphs=args.graphs,
    )
    driver.run()


if __name__ == "__main__":
    main()
