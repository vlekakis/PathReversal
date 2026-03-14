"""Unit tests for the PathReversal algorithm."""

import pytest
from path_reversal.algorithm import PathReversal
from path_reversal.status import PRStatus, PRNext
from path_reversal.message import MsgType
from tests.conftest import make_request_msg


class TestInitialization:
    """Test initial state setup."""

    def test_initial_state_with_token(self, make_node):
        """Node initialized with item should be EATING."""
        node = make_node(item_holder="holder", item="TOKEN")
        assert node.status == PRStatus.EATING
        assert node.data == "TOKEN"
        assert node.last is None
        assert node.next is None

    def test_initial_state_without_token(self, make_node):
        """Node without item should be THINKING with last pointing to holder."""
        node = make_node(item_holder="holder")
        assert node.status == PRStatus.THINKING
        assert node.data is None
        assert node.last == "holder"
        assert node.next is None

    def test_request_queue_initialized_empty(self, make_node):
        """Request queue should start empty."""
        node = make_node(item_holder="holder")
        assert len(node.request_queue) == 0


class TestBecomeHungry:
    """Test THINKING -> HUNGRY transition."""

    def test_become_hungry_from_thinking(self, make_node):
        """Should send request to last and clear last pointer."""
        node = make_node(item_holder="holder")
        result = node.becomeHungry("myself")
        assert result is not False
        msg, dest = result
        assert dest == "holder"
        assert node.status == PRStatus.HUNGRY
        assert node.last is None

    def test_become_hungry_when_not_thinking(self, make_node):
        """Should return False if already HUNGRY or EATING."""
        node = make_node(item_holder="holder", item="TOKEN")
        assert node.status == PRStatus.EATING
        result = node.becomeHungry("myself")
        assert result is False

    def test_become_hungry_when_already_hungry(self, make_node):
        """Should return False if already HUNGRY."""
        node = make_node(item_holder="holder")
        node.becomeHungry("myself")
        assert node.status == PRStatus.HUNGRY
        result = node.becomeHungry("myself")
        assert result is False


class TestRecv:
    """Test the core recv() method — request forwarding and token serving."""

    def test_recv_as_token_holder(self, make_node):
        """Token holder should give up token and become THINKING."""
        node = make_node(item_holder=None, item="TOKEN")
        msg = make_request_msg("requester")

        action, data = node.recv("self", msg)

        assert action == PRNext.TX_OBJ
        assert data == "TOKEN"
        assert node.status == PRStatus.THINKING
        assert node.data is None

    def test_recv_forwards_along_last(self, make_node):
        """THINKING node with last pointer should forward and reverse edge."""
        node = make_node(item_holder="holder")
        msg = make_request_msg("requester")

        action, target = node.recv("self", msg)

        assert action == PRNext.FORWARD
        assert target == "holder"
        # Edge reversal: last now points to requester
        assert node.last == "requester"

    def test_recv_when_hungry_queues_request(self, make_node):
        """HUNGRY node (last=None) should queue the request."""
        node = make_node(item_holder="holder")
        node.becomeHungry("myself")
        assert node.last is None

        msg = make_request_msg("other_requester")
        action, _ = node.recv("self", msg)

        assert action == PRNext.QUEUED
        assert len(node.request_queue) == 1
        assert node.request_queue[0] == "other_requester"

    def test_recv_queues_multiple_requests(self, make_node):
        """Multiple requests at a hungry node should all be queued."""
        node = make_node(item_holder="holder")
        node.becomeHungry("myself")

        for i in range(3):
            msg = make_request_msg(f"requester_{i}")
            action, _ = node.recv("self", msg)
            assert action == PRNext.QUEUED

        assert len(node.request_queue) == 3

    def test_recv_token_holder_sets_last_to_requester(self, make_node):
        """After giving up token, last should point to requester."""
        node = make_node(item_holder=None, item="TOKEN")
        msg = make_request_msg("requester")
        node.recv("self", msg)
        assert node.last == "requester"


class TestBecomeThinking:
    """Test EATING -> THINKING transition with queue draining."""

    def test_become_thinking_with_next(self, make_node):
        """Should return (next_node, data) when next is set."""
        node = make_node(item_holder=None, item="TOKEN")
        node.next = "waiting_node"

        result = node.becomeThinking()

        assert result is not None
        next_node, data = result
        assert next_node == "waiting_node"
        assert data == "TOKEN"
        assert node.status == PRStatus.THINKING
        assert node.data is None
        assert node.next is None

    def test_become_thinking_drains_queue(self, make_node):
        """Should serve first queued requester if next is None."""
        node = make_node(item_holder=None, item="TOKEN")
        node.request_queue.append("queued_1")
        node.request_queue.append("queued_2")

        result = node.becomeThinking()

        assert result is not None
        next_node, data = result
        assert next_node == "queued_1"
        assert data == "TOKEN"
        # Second request still in queue
        assert len(node.request_queue) == 1
        assert node.request_queue[0] == "queued_2"

    def test_become_thinking_no_waiting(self, make_node):
        """Should return None when no one is waiting."""
        node = make_node(item_holder=None, item="TOKEN")
        # Need to set next so we can become thinking
        # Actually, becomeThinking checks for EATING state, not next
        result = node.becomeThinking()
        assert result is None
        assert node.status == PRStatus.THINKING

    def test_become_thinking_when_not_eating_raises(self, make_node):
        """Should raise RuntimeError if not EATING."""
        node = make_node(item_holder="holder")
        assert node.status == PRStatus.THINKING
        with pytest.raises(RuntimeError):
            node.becomeThinking()

    def test_become_thinking_updates_last(self, make_node):
        """After forwarding token, last should point where token went."""
        node = make_node(item_holder=None, item="TOKEN")
        node.next = "next_node"
        node.becomeThinking()
        assert node.last == "next_node"


class TestReset:
    """Test state reset."""

    def test_reset_clears_all_state(self, make_node):
        """Reset should return to clean THINKING state."""
        node = make_node(item_holder=None, item="TOKEN")
        node.request_queue.append("someone")
        node.next = "someone"

        node.reset()

        assert node.status == PRStatus.THINKING
        assert node.data is None
        assert node.last is None
        assert node.next is None
        assert len(node.request_queue) == 0


class TestMutualExclusionInvariant:
    """Test that the algorithm maintains mutual exclusion across multiple nodes."""

    def test_single_hungry_gets_token(self, ring_of_nodes):
        """One hungry node should eventually get the token."""
        nodes, names = ring_of_nodes(4, holder_index=0)
        holder_name = names[0]
        requester_name = names[1]

        # Node 1 becomes hungry, sends request to holder (node 0)
        result = nodes[requester_name].becomeHungry(requester_name)
        assert result is not False

        # Request arrives at holder
        msg = make_request_msg(requester_name)
        action, data = nodes[holder_name].recv(holder_name, msg)

        assert action == PRNext.TX_OBJ
        assert data == "TOKEN"

        # Requester receives the token
        nodes[requester_name].becomeEating(data)

        # Verify mutual exclusion
        eating = [n for n, node in nodes.items() if node.status == PRStatus.EATING]
        assert len(eating) == 1
        assert eating[0] == requester_name

    def test_two_hungry_nodes_sequential(self, ring_of_nodes):
        """Two hungry nodes should get the token one at a time."""
        nodes, names = ring_of_nodes(4, holder_index=0)
        holder = names[0]
        req1 = names[1]
        req2 = names[2]

        # Both become hungry
        nodes[req1].becomeHungry(req1)
        nodes[req2].becomeHungry(req2)

        # req1's request reaches holder first
        msg1 = make_request_msg(req1)
        action, data = nodes[holder].recv(holder, msg1)
        assert action == PRNext.TX_OBJ

        # req1 eats
        nodes[req1].becomeEating(data)
        eating = [n for n, node in nodes.items() if node.status == PRStatus.EATING]
        assert eating == [req1]

        # req2's request reaches holder (who no longer has token)
        msg2 = make_request_msg(req2)
        action, target = nodes[holder].recv(holder, msg2)
        assert action == PRNext.FORWARD
        assert target == req1  # holder's last now points to req1

        # Request forwarded to req1
        msg2_fwd = make_request_msg(req2)
        # req1 finishes eating, gives up token
        action, data = nodes[req1].recv(req1, msg2_fwd)
        assert action == PRNext.TX_OBJ

        # req2 eats
        nodes[req2].becomeEating(data)
        eating = [n for n, node in nodes.items() if node.status == PRStatus.EATING]
        assert eating == [req2]

    def test_hungry_node_queues_then_serves(self, ring_of_nodes):
        """A hungry node that receives a request should queue it,
        then serve it after getting the token itself."""
        nodes, names = ring_of_nodes(3, holder_index=0)
        holder = names[0]
        req1 = names[1]
        req2 = names[2]

        # req1 becomes hungry (last was holder, now None)
        nodes[req1].becomeHungry(req1)
        assert nodes[req1].last is None

        # req2 also becomes hungry, sends to holder
        nodes[req2].becomeHungry(req2)

        # Simulate: req2's request gets forwarded through the ring
        # and arrives at req1 (which is hungry)
        msg_from_req2 = make_request_msg(req2)
        action, _ = nodes[req1].recv(req1, msg_from_req2)
        assert action == PRNext.QUEUED
        assert len(nodes[req1].request_queue) == 1

        # Meanwhile, req1's original request reaches the holder
        msg_from_req1 = make_request_msg(req1)
        action, data = nodes[holder].recv(holder, msg_from_req1)
        assert action == PRNext.TX_OBJ

        # req1 gets the token and eats
        nodes[req1].becomeEating(data)
        assert nodes[req1].status == PRStatus.EATING

        # req1 finishes eating — becomeThinking should drain the queue
        result = nodes[req1].becomeThinking()
        assert result is not None
        next_node, token = result
        assert next_node == req2
        assert token == data

        # req2 gets the token
        nodes[req2].becomeEating(token)
        eating = [n for n, node in nodes.items() if node.status == PRStatus.EATING]
        assert eating == [req2]


class TestGetStatus:
    """Test status reporting."""

    def test_status_increments_seq(self, make_node):
        """Each call to getStatus should increment the sequence number."""
        node = make_node(item_holder="holder")
        s1 = node.getStatus()
        s2 = node.getStatus()
        assert s2[0] == s1[0] + 1

    def test_status_reflects_current_state(self, make_node):
        """Status tuple should match current node state."""
        node = make_node(item_holder="holder")
        seq, status, next_val, last_val = node.getStatus()
        assert status == PRStatus.THINKING
        assert last_val == "holder"
        assert next_val is None
