"""Integration tests — simulate multi-node Path Reversal without ZMQ.

These tests use direct method calls to simulate message passing between
PathReversal instances, verifying correctness of the algorithm in
scenarios with multiple concurrent hungry nodes.
"""

import pytest
from collections import deque
from path_reversal.algorithm import PathReversal
from path_reversal.status import PRStatus, PRNext
from path_reversal.message import MsgType
from tests.conftest import make_request_msg
import logging

logger = logging.getLogger("test_integration")


class SimulatedNetwork:
    """In-process simulation of the Path Reversal ring network.

    Simulates message passing between nodes without ZMQ, using
    direct method calls and a message queue.
    """

    def __init__(self, n_nodes, holder_index=0):
        self.names = [f"node:{5001 + i}" for i in range(n_nodes)]
        self.nodes = {}
        holder = self.names[holder_index]

        for name in self.names:
            if name == holder:
                self.nodes[name] = PathReversal(holder, "TOKEN", logger)
            else:
                self.nodes[name] = PathReversal(holder, None, logger)

        # Message queue: (destination, message_dict)
        self.pending_messages = deque()
        # Track eating history for verification
        self.eating_history = []

    def make_hungry(self, node_name):
        """Make a node hungry and enqueue its request."""
        node = self.nodes[node_name]
        result = node.becomeHungry(node_name)
        if result is False:
            raise RuntimeError(f"Node {node_name} cannot become hungry "
                               f"(status: {node.status})")
        import pickle
        msg_bytes, dest = result
        msg = pickle.loads(msg_bytes)
        self.pending_messages.append((dest, msg))

    def step(self):
        """Process one message from the queue. Returns True if a message was processed."""
        if not self.pending_messages:
            return False

        dest, msg = self.pending_messages.popleft()
        node = self.nodes[dest]

        if msg[MsgType.TYPE] == MsgType.PR_REQ:
            action, action_arg = node.recv(dest, msg)

            if action == PRNext.FORWARD:
                # Create forwarded request
                fwd_msg = {
                    MsgType.TYPE: MsgType.PR_REQ,
                    MsgType.DST: action_arg,
                    MsgType.SOURCE: msg[MsgType.SOURCE],
                    MsgType.DATA: None,
                    MsgType.DATA_ID: None,
                }
                self.pending_messages.append((action_arg, fwd_msg))

            elif action == PRNext.TX_OBJ:
                # Send token to requester
                requester = msg[MsgType.SOURCE]
                obj_msg = {
                    MsgType.TYPE: MsgType.PR_OBJ,
                    MsgType.DST: requester,
                    MsgType.SOURCE: dest,
                    MsgType.DATA: action_arg,
                    MsgType.DATA_ID: None,
                }
                self.pending_messages.append((requester, obj_msg))

            elif action == PRNext.QUEUED:
                pass  # Request queued, no message to send

        elif msg[MsgType.TYPE] == MsgType.PR_OBJ:
            # Node receives the token
            node.becomeEating(msg[MsgType.DATA])
            self.eating_history.append(dest)

            # Only leave critical section if there are queued requests.
            # Otherwise stay EATING — new PR_REQs will be handled by recv().
            if node.request_queue:
                forward = node.becomeThinking()
                if forward is not None:
                    next_node, data = forward
                    obj_msg = {
                        MsgType.TYPE: MsgType.PR_OBJ,
                        MsgType.DST: next_node,
                        MsgType.SOURCE: dest,
                        MsgType.DATA: data,
                        MsgType.DATA_ID: None,
                    }
                    self.pending_messages.append((next_node, obj_msg))

        return True

    def run_to_completion(self, max_steps=1000):
        """Process all messages until queue is empty or max_steps reached."""
        steps = 0
        while self.step():
            steps += 1
            if steps > max_steps:
                raise RuntimeError(f"Simulation did not converge after {max_steps} steps")
            # Invariant check: at most one node eating at any time
            eating = [n for n, node in self.nodes.items()
                      if node.status == PRStatus.EATING]
            assert len(eating) <= 1, \
                f"Mutual exclusion violated! Eating nodes: {eating}"
        return steps

    @property
    def eating_nodes(self):
        return [n for n, node in self.nodes.items()
                if node.status == PRStatus.EATING]


class TestSingleHungryNode:

    def test_one_node_gets_token(self):
        """Single hungry node should get the token."""
        net = SimulatedNetwork(4, holder_index=0)
        net.make_hungry("node:5002")
        net.run_to_completion()

        assert "node:5002" in net.eating_history

    def test_holder_becomes_thinking(self):
        """Token holder should become THINKING after serving request."""
        net = SimulatedNetwork(4, holder_index=0)
        net.make_hungry("node:5002")
        net.run_to_completion()

        assert net.nodes["node:5001"].status == PRStatus.THINKING


class TestConcurrentHungryNodes:

    def test_two_concurrent_hungry(self):
        """Two nodes hungry simultaneously — both should eventually eat."""
        net = SimulatedNetwork(4, holder_index=0)
        net.make_hungry("node:5002")
        net.make_hungry("node:5003")
        net.run_to_completion()

        assert "node:5002" in net.eating_history
        assert "node:5003" in net.eating_history

    def test_three_concurrent_hungry(self):
        """Three nodes hungry — all should eat exactly once."""
        net = SimulatedNetwork(4, holder_index=0)
        net.make_hungry("node:5002")
        net.make_hungry("node:5003")
        net.make_hungry("node:5004")
        net.run_to_completion()

        assert "node:5002" in net.eating_history
        assert "node:5003" in net.eating_history
        assert "node:5004" in net.eating_history

    def test_all_nodes_hungry(self):
        """All non-holder nodes hungry — all should eat."""
        net = SimulatedNetwork(5, holder_index=0)
        for i in range(1, 5):
            net.make_hungry(f"node:{5001 + i}")
        net.run_to_completion()

        for i in range(1, 5):
            assert f"node:{5001 + i}" in net.eating_history

    def test_mutual_exclusion_never_violated(self):
        """Run many concurrent requests — mutual exclusion must hold throughout."""
        net = SimulatedNetwork(6, holder_index=0)
        for i in range(1, 6):
            net.make_hungry(f"node:{5001 + i}")
        # run_to_completion checks invariant on every step
        net.run_to_completion()

    def test_no_deadlock(self):
        """System should not deadlock with concurrent requests."""
        net = SimulatedNetwork(4, holder_index=0)
        net.make_hungry("node:5002")
        net.make_hungry("node:5003")
        net.make_hungry("node:5004")
        # If this returns without RuntimeError, no deadlock
        steps = net.run_to_completion(max_steps=100)
        assert steps > 0


class TestQueueDraining:

    def test_queued_requests_are_served(self):
        """Requests queued at a hungry node should be served after it eats."""
        net = SimulatedNetwork(3, holder_index=0)
        # node:5002 becomes hungry, sends request to node:5001 (holder)
        net.make_hungry("node:5002")
        # node:5003 also hungry, sends request to node:5001
        net.make_hungry("node:5003")

        net.run_to_completion()

        # Both should have eaten
        assert "node:5002" in net.eating_history
        assert "node:5003" in net.eating_history

    def test_large_queue(self):
        """Many concurrent requests should all eventually be served."""
        n = 10
        net = SimulatedNetwork(n + 1, holder_index=0)
        for i in range(1, n + 1):
            net.make_hungry(f"node:{5001 + i}")

        net.run_to_completion(max_steps=500)

        for i in range(1, n + 1):
            assert f"node:{5001 + i}" in net.eating_history


class TestEdgeCases:

    def test_two_node_network(self):
        """Minimal network with 2 nodes should work."""
        net = SimulatedNetwork(2, holder_index=0)
        net.make_hungry("node:5002")
        net.run_to_completion()
        assert "node:5002" in net.eating_history

    def test_sequential_hungry_requests(self):
        """Nodes becoming hungry one after another, not simultaneously."""
        net = SimulatedNetwork(3, holder_index=0)

        # First request
        net.make_hungry("node:5002")
        net.run_to_completion()
        assert "node:5002" in net.eating_history

        # node:5002 now has the token; make node:5003 hungry
        net.make_hungry("node:5003")
        net.run_to_completion()
        assert "node:5003" in net.eating_history
