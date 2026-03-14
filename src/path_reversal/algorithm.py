"""Core Path Reversal algorithm for distributed mutual exclusion.

The Path Reversal algorithm manages access to a shared resource (token)
among distributed nodes. Nodes form a directed graph where edges point
toward the token holder. When a node wants the token, it sends a request
along its edge, reversing the edge direction as the request passes through.

States:
    THINKING - Node is idle, not interested in the token
    HUNGRY   - Node wants the token, has sent a request
    EATING   - Node has the token (in critical section)

Invariant: Exactly one node is EATING at any time.
"""

import pickle
import logging
from collections import deque
from typing import Any, Optional, Tuple, Union

from path_reversal.message import MsgType, MsgFactory
from path_reversal.status import PRStatus, PRNext

logger = logging.getLogger(__name__)


class PRStatusUpdate:
    """Status update messages sent from nodes to the driver for verification."""
    NEXT = "next"
    LAST = "last"
    STATUS = "status"
    SEQ = "SEQ"
    SATISFY = "SATISFY"

    @staticmethod
    def createStatusUpdate(mtype: int, seqNum: int, status: int,
                           next_node: Optional[str], last_node: Optional[str],
                           satisfy: bool) -> bytes:
        statusUpdate = {
            MsgType.TYPE: mtype,
            PRStatusUpdate.SEQ: seqNum,
            PRStatusUpdate.STATUS: status,
            PRStatusUpdate.NEXT: next_node,
            PRStatusUpdate.LAST: last_node,
            PRStatusUpdate.SATISFY: satisfy,
        }
        return pickle.dumps(statusUpdate)

    @staticmethod
    def createStatusACKMessage(seqNum: int) -> bytes:
        statusAck = {
            MsgType.TYPE: MsgType.PR_STATUS_ACK,
            PRStatusUpdate.SEQ: seqNum,
        }
        return pickle.dumps(statusAck)


class PathReversal:
    """Path Reversal algorithm instance for a single node.

    Each node maintains:
        last: Points toward where the token was last known to be.
              Requests are forwarded along this pointer.
        next: Points to the node waiting to receive the token after us.
        data: The token/resource (non-None only when EATING).
        request_queue: Queued requests from other nodes when this node
                       is HUNGRY and cannot forward.
    """

    def __init__(self, item_holder: Optional[str], item: Any,
                 node_logger: Any = None):
        self.request_queue: deque = deque()
        self.seq: int = 0
        self._logger = node_logger or logger
        self.set(item_holder, item)

    @property
    def log(self):
        return self._logger

    def becomeHungry(self, issuerNetName: str) -> Union[Tuple[bytes, str], bool]:
        """Transition from THINKING to HUNGRY.

        Sends a PR_REQ along the `last` pointer and clears it.

        Returns:
            (request_message, destination) if successful
            False if not in THINKING state
        """
        if self.status != PRStatus.THINKING:
            return False

        self.status = PRStatus.HUNGRY
        prReq = MsgFactory.create(MsgType.PR_REQ,
                                  dst=self.last, src=issuerNetName)
        self.log.debug("\t[PR-LOG] Status: HUNGRY")

        last = self.last
        self.last = None
        return prReq, last

    def becomeThinking(self) -> Optional[Tuple[str, Any]]:
        """Transition from EATING to THINKING.

        Checks self.next and the request_queue for waiting requesters.

        Returns:
            (next_node, data) if the token should be forwarded
            None if no one is waiting
        Raises:
            RuntimeError if not currently EATING
        """
        if self.status != PRStatus.EATING:
            raise RuntimeError("Cannot become THINKING: not currently EATING")

        data = self.data

        # Determine who gets the token next
        next_node = self.next
        if next_node is None and self.request_queue:
            next_node = self.request_queue.popleft()

        self.status = PRStatus.THINKING
        self.next = None
        self.data = None
        self.log.debug("\t[PR-LOG] Status: THINKING")

        if next_node is not None:
            # Update last to point toward where the token is going
            self.last = next_node
            return (next_node, data)
        return None

    def getStatus(self) -> tuple:
        """Get current status for reporting to the driver."""
        status = (self.seq, self.status, self.next, self.last)
        self.seq += 1
        return status

    def reset(self):
        """Reset to initial state."""
        self.data = None
        self.next = None
        self.last = None
        self.request_queue.clear()
        self.status = PRStatus.THINKING

    def set(self, item_holder: Optional[str], item: Any,
            node_logger: Any = None):
        """Initialize or reinitialize the node state.

        Args:
            item_holder: Network name of the node holding the token.
            item: The token data (non-None if this node holds it).
            node_logger: Optional logger override.
        """
        if node_logger is not None:
            self._logger = node_logger
        self.request_queue = deque()
        if item is not None:
            self.last = None
            self.next = None
            self.becomeEating(item)
        else:
            self.last = item_holder
            self.next = None
            self.data = None
            self.status = PRStatus.THINKING

    def becomeEating(self, data: Any):
        """Transition to EATING state with the given token data."""
        self.log.debug("\t[PR-LOG] Status: EATING data:" + str(data))
        self.status = PRStatus.EATING
        self.data = data

    def recv(self, forwarderNetName: str, msg: dict) -> Tuple[PRNext, Any]:
        """Handle an incoming PR_REQ message.

        This is the core of the path reversal algorithm:
        1. If we have the token: give it up, reverse pointers
        2. If we have a `last` pointer: forward request, reverse edge
        3. If we're hungry (last=None, data=None): queue the request

        Returns:
            (PRNext.FORWARD, target_node) - forward the request
            (PRNext.TX_OBJ, data) - send the token to the requester
            (PRNext.QUEUED, None) - request was queued
        """
        requester = msg[MsgType.SOURCE]

        if self.data is None:
            # We don't have the token
            if self.last is not None:
                # Forward the request along `last`, reverse the edge
                toNode = self.last
                self.last = requester
                return (PRNext.FORWARD, toNode)
            else:
                # We're hungry too — queue this request
                self.request_queue.append(requester)
                self.log.debug("\t[PR-LOG] Queued request from: " + str(requester))
                return (PRNext.QUEUED, None)
        else:
            # We have the token — serve it to the requester
            data = self.data
            self.next = requester
            self.last = requester
            self.becomeThinking()
            return (PRNext.TX_OBJ, data)
