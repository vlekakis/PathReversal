import pickle
from collections import deque
from Message import MsgType
from Message import MsgFactory


class PRStatusUpdate:
    NEXT = "next"
    LAST = "last"
    STATUS = "status"
    SEQ = "SEQ"
    SATISFY = "SATISFY"

    @staticmethod
    def createStatusUpdate(mtype, seqNum, status, next, last, satisfy):
        statusUpdate = {}
        statusUpdate[MsgType.TYPE] = mtype
        statusUpdate[PRStatusUpdate.SEQ] = seqNum
        statusUpdate[PRStatusUpdate.STATUS] = status
        statusUpdate[PRStatusUpdate.NEXT] = next
        statusUpdate[PRStatusUpdate.LAST] = last
        statusUpdate[PRStatusUpdate.SATISFY] = satisfy
        statusUpdate = pickle.dumps(statusUpdate)
        return statusUpdate

    @staticmethod
    def createStatusACKMessage(seqNum):
        statusAck = {}
        statusAck[MsgType.TYPE] = MsgType.PR_STATUS_ACK
        statusAck[PRStatusUpdate.SEQ] = seqNum
        statusAck = pickle.dumps(statusAck)
        return statusAck


class PRStatus:
    THINKING = 0
    HUNGRY = 1
    EATING = 2


class PRNext:
    FORWARD = 3
    RX_OBJ = 4
    TX_OBJ = 5
    QUEUED = 6


class PathReversal:

    def __init__(self, itemHolder, item, logger):
        self.request_queue = deque()
        self.seq = 0
        self.set(itemHolder, item, logger)

    def becomeHungry(self, issuerNetName):
        if self.status == PRStatus.THINKING:
            self.status = PRStatus.HUNGRY
            prReq = MsgFactory.create(MsgType.PR_REQ,
                                    dst=self.last, src=issuerNetName)
            self.logger.debug("\t[PR-LOG] Status: HUNGRY")

            last = self.last
            self.last = None
            return prReq, last
        return False

    def becomeThinking(self):
        """Transition from EATING to THINKING.

        Returns (next_node, data) if the token should be forwarded
        to a waiting requester, or None if no one is waiting.
        """
        if self.status != PRStatus.EATING:
            raise RuntimeError("Cannot become THINKING: not currently EATING")

        data = self.data

        # Determine who gets the token next: self.next first, then queued requests
        next_node = self.next
        if next_node is None and self.request_queue:
            next_node = self.request_queue.popleft()

        self.status = PRStatus.THINKING
        self.next = None
        self.data = None
        self.logger.debug("\t[PR-LOG] Status: THINKING")

        if next_node is not None:
            return (next_node, data)
        return None

    def getStatus(self):
        status = (self.seq, self.status, self.next, self.last)
        self.seq += 1
        return status

    def reset(self):
        self.data = None
        self.next = None
        self.last = None
        self.request_queue.clear()
        self.status = PRStatus.THINKING

    def set(self, itemHolder, item, logger):
        self.logger = logger
        self.request_queue = deque()
        if item is not None:
            self.last = None
            self.next = None
            self.becomeEating(item)
        else:
            self.last = itemHolder
            self.next = None
            self.data = None
            self.status = PRStatus.THINKING

    def becomeEating(self, data):
        self.logger.debug("\t[PR-LOG] Status: EATING data:" + str(data))
        self.status = PRStatus.EATING
        self.data = data

    def recv(self, forwarderNetName, msg):
        """Handle an incoming PR_REQ message.

        Returns a tuple (action, actionArg) where:
        - (PRNext.FORWARD, target_node) - forward the request to target_node
        - (PRNext.TX_OBJ, data) - send the data object to the requester
        - (PRNext.QUEUED, None) - request was queued (node is hungry, can't forward)
        """
        requester = msg[MsgType.SOURCE]

        if self.data is None:
            # We don't have the token
            if self.last is not None:
                # We know where to forward — reverse the edge
                toNode = self.last
                self.last = requester
                return (PRNext.FORWARD, toNode)
            else:
                # We're hungry too (last=None) — queue the request
                self.request_queue.append(requester)
                self.logger.debug("\t[PR-LOG] Queued request from: " + str(requester))
                return (PRNext.QUEUED, None)

        else:
            # We have the token — give it to the requester
            data = self.data
            self.next = requester
            self.last = requester
            result = self.becomeThinking()
            # becomeThinking returns (next_node, data) — the token goes to next_node
            # which should be `requester` since we just set self.next = requester
            return (PRNext.TX_OBJ, data)
