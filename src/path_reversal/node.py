"""FifoNode — individual node process for the Path Reversal system.

Each node runs as a separate process with ZMQ sockets for:
- Subscribing to driver commands (PUB/SUB)
- Communicating with the driver sink (REQ/REP)
- Peer-to-peer communication in a ring (REQ/REP)
"""

import zmq
import logging
import pickle
import argparse
from path_reversal.status import PRNext
from path_reversal.algorithm import PathReversal, PRStatusUpdate
from zmq.eventloop import ioloop
from zmq.eventloop import zmqstream
from path_reversal.message import MsgType, MsgFactory
from copy import deepcopy
from queue import Queue, Empty
from random import choice
from time import sleep

logger = logging.getLogger(__name__)


class FifoStats:
    def __init__(self):
        self.tx = {}
        self.rx = {}
        self.n = 0
        self.conStatus = {}

    def __repr__(self):
        return f"FifoStats(tx={self.tx}, rx={self.rx})"


class FifoNode:

    def __init__(self):
        self.fifoStats = None
        self.nodeIloop = None

    def getDest(self, msg):
        m = pickle.loads(msg[1])
        return m[MsgType.DST]

    def updateFifoStats(self, dest, msg, tx=False, rx=False):
        key = repr((self.name, dest))
        if tx:
            if key not in self.fifoStats.tx:
                self.fifoStats.tx[key] = []
            self.fifoStats.tx[key].append(msg)
        elif rx:
            if key not in self.fifoStats.rx:
                self.fifoStats.rx[key] = []
            self.fifoStats.rx[key].append(msg)

    def procPeerTxServerMsg(self, stream, msg, status):
        dst = self.getDest(msg)
        logger.debug("Server replying to: %s", dst)
        self.updateFifoStats(dst, msg, tx=True)

    def ackAgent(self, satisfy=False):
        prNodeStatus = self.prMod.getStatus()
        prNodeStatusMsg = PRStatusUpdate.createStatusUpdate(
            MsgType.PR_STATUS_UPDATE,
            prNodeStatus[0], prNodeStatus[1],
            prNodeStatus[2], prNodeStatus[3],
            satisfy)
        logger.debug("[PR-LOG] Sending Status update to Agent (seq=%d)", prNodeStatus[0])

        self.sinkSocket.send_multipart([self.name.encode(), prNodeStatusMsg])
        updateAck = self.sinkSocket.recv_multipart()
        updateAck = pickle.loads(updateAck[1])
        if prNodeStatus[0] != updateAck[PRStatusUpdate.SEQ]:
            raise RuntimeError(
                f"Status ACK seq mismatch: expected {prNodeStatus[0]}, "
                f"got {updateAck[PRStatusUpdate.SEQ]}")

    def procPeerRxPathReversalMsg(self, stream, rxMsg, sender, pureMsg):
        logger.debug("[PR-LOG] Sending PR_ACK to: %s", sender)
        ackMsg = MsgFactory.create(MsgType.PR_ACK, dst=sender)
        stream.send_multipart([self.name.encode(), ackMsg])

        if rxMsg[MsgType.DST] != self.name:
            if rxMsg[MsgType.TYPE] == MsgType.PR_REQ:
                logger.debug("[PR-LOG] Forwarding PR_REQ to: %s", self.neighbor)
            elif rxMsg[MsgType.TYPE] == MsgType.PR_OBJ:
                logger.debug("[PR-LOG] Forwarding PR_OBJ to: %s", self.neighbor)
            self.peerCltStream.send_multipart([self.name.encode(), pureMsg])

        elif rxMsg[MsgType.DST] == self.name:

            if rxMsg[MsgType.TYPE] == MsgType.PR_REQ:
                logger.debug("[PR-LOG] PR_REQ reached DST")
                action, actionArg = self.prMod.recv(self.name, rxMsg)

                self.ackAgent()

                if action == PRNext.FORWARD:
                    logger.debug("[PR-LOG] New Request FWD to: %s", actionArg)
                    txMsg = MsgFactory.create(MsgType.PR_REQ,
                                              dst=actionArg,
                                              src=rxMsg[MsgType.SOURCE])
                    self.peerCltStream.send_multipart([self.name.encode(), txMsg])

                elif action == PRNext.TX_OBJ:
                    logger.debug("[PR-LOG] Serving OBJECT to: %s", rxMsg[MsgType.SOURCE])
                    txMsg = MsgFactory.create(MsgType.PR_OBJ,
                                              dst=rxMsg[MsgType.SOURCE],
                                              data=actionArg,
                                              src=self.name)
                    self.peerCltStream.send_multipart([self.name.encode(), txMsg])

                elif action == PRNext.QUEUED:
                    logger.debug("[PR-LOG] Request from %s queued (node is hungry)",
                                 rxMsg[MsgType.SOURCE])

            elif rxMsg[MsgType.TYPE] == MsgType.PR_OBJ:
                logger.debug("[PR-LOG] Received OBJECT from: %s", rxMsg[MsgType.SOURCE])
                self.prMod.becomeEating(rxMsg[MsgType.DATA])

                logger.debug("[PR-LOG] Inform the server about EATING")
                self.ackAgent(satisfy=True)

                # If there are queued requests, serve them immediately
                if self.prMod.request_queue:
                    forward_info = self.prMod.becomeThinking()
                    if forward_info is not None:
                        next_node, data = forward_info
                        logger.debug("[PR-LOG] Forwarding token to queued requester: %s", next_node)
                        self.ackAgent()
                        txMsg = MsgFactory.create(MsgType.PR_OBJ,
                                                  dst=next_node,
                                                  data=data,
                                                  src=self.name)
                        self.peerCltStream.send_multipart([self.name.encode(), txMsg])

    def procPeerRxServerMsg(self, stream, msg):
        rxMsg = pickle.loads(msg[1])

        if rxMsg[MsgType.TYPE] in (MsgType.PR_REQ, MsgType.PR_OBJ):
            self.procPeerRxPathReversalMsg(stream, rxMsg, msg[0], msg[1])

        if rxMsg[MsgType.TYPE] == MsgType.KEEP_ALIVE:
            logger.debug("Received KEEP_ALIVE from: %s", msg[0])
            msgOut = MsgFactory.create(MsgType.KEEP_ALIVE_ACK, msg[0])
            try:
                stream.send_multipart([self.name.encode(), msgOut])
            except TypeError as e:
                logger.debug("Error sending KEEP_ALIVE_ACK: %s", e)

        if rxMsg[MsgType.TYPE] == MsgType.DATA_MSG:
            did = rxMsg[MsgType.DATA_ID]
            logger.debug("Received DATA_MSG from %s with id %s", msg[0], did)
            msgOut = MsgFactory.create(MsgType.DATA_ACK, None, None, did, None)
            stream.send_multipart([self.name.encode(), msgOut])

            if rxMsg[MsgType.DST] == self.name:
                self.dataObject = deepcopy(rxMsg[MsgType.DATA])
                self.dataObjectId = rxMsg[MsgType.DATA_ID]
                logger.debug("DATA_MSG destination reached with id: %s", self.dataObjectId)
                msgOutDatAck = MsgFactory.create(MsgType.DATA_ACK, None, None, did, None)
                self.streamCmdOut.send_multipart([self.name.encode(), msgOutDatAck])
            else:
                logger.debug("Forwarding DATA_MSG(%s) to neighbor: %s", did, self.neighbor)
                self.peerCltStream.send_multipart([self.name.encode(), msg[1]])

    def procPeerTxClientMsg(self, msg, status):
        randomWaitOnFifo = choice(range(3))
        sleep(randomWaitOnFifo)
        dst = self.getDest(msg)
        logger.debug("Client sending to: %s", dst)
        self.updateFifoStats(dst, msg, tx=True)

    def procPeerRxClientMsg(self, msg):
        if len(msg) > 1:
            rxMsg = pickle.loads(msg[1])
            self.updateFifoStats(msg[0], msg, rx=True)

            if rxMsg[MsgType.TYPE] == MsgType.KEEP_ALIVE_ACK:
                logger.debug("Received KEEP_ALIVE_ACK from: %s", msg[0])
            if rxMsg[MsgType.TYPE] == MsgType.DATA_ACK:
                logger.debug("Received DATA_MSG_ACK for: %s", rxMsg[MsgType.DATA_ID])
            if rxMsg[MsgType.TYPE] == MsgType.PR_ACK:
                logger.debug("[PR-LOG] Received ACK for PR-REQ/PR_OBJ")

    def ackOrForward(self, msgIn, caseExisting=False, caseNACK=False):
        did = msgIn[MsgType.DATA_ID]
        if msgIn[MsgType.DST] == self.name or caseNACK:
            if not caseNACK:
                msgOut = MsgFactory.create(MsgType.DATA_ACK, dataId=did)
                logger.debug("Object %s is home. Sending ACK to agent", did)
            else:
                msgOut = MsgFactory.create(MsgType.DATA_NACK, dataId=did)
                logger.debug("Object %s is NOT home. Sending NACK to agent", did)

            self.streamCmdOut.send_multipart([self.name.encode(), msgOut])
            self.streamCmdOut.flush()
        else:
            if caseExisting:
                msgIn[MsgType.TYPE] = MsgType.DATA_MSG

            msgOut = [self.name.encode(), pickle.dumps(msgIn)]
            logger.debug("Forwarding DATA_MSG(%s) to neighbor: %s", did, self.neighbor)
            self.peerSockClt.send_multipart(msgOut)

    def procAgentCmd(self, stream, msg):
        # Decode bytes frames to strings for comparison
        msg = [m.decode() if isinstance(m, bytes) else m for m in msg]

        if msg[0] == 'Exit':
            logger.debug("Received exit")
            stream.stop_on_recv()
            self.nodeIloop.stop()

        if msg[0] == 'ConnectToNeighbor':
            logger.debug("ConnectingToNeighbor CMD arrived")
            self.peerSockClt = self.context.socket(zmq.REQ)
            self.peerSockClt.connect(self.neighborAddr)
            self.peerCltStream = zmqstream.ZMQStream(self.peerSockClt)
            self.peerCltStream.on_recv(self.procPeerRxClientMsg)
            self.peerCltStream.on_send(self.procPeerTxClientMsg)

        if msg[0] == 'TestConnectionToNeighbor':
            logger.debug("TestConnection With the Peer-Neighbor")
            msgOut = MsgFactory.create(MsgType.KEEP_ALIVE, self.neighbor)
            self.peerSockClt.send_multipart([self.name.encode(), msgOut])

        if msg[0] == 'Reset':
            logger.debug("Server send RESET message")
            self.prMod.reset()

        if msg[0] == 'Echo':
            logger.debug("Server sends echo message: %s", msg[1] if len(msg) > 1 else "")

        if msg[0] == 'Set':
            logger.debug("Server send SET message")
            payload = msg[1] if isinstance(msg[1], bytes) else msg[1].encode()
            rxMsg = pickle.loads(payload)
            itemHolder = rxMsg[MsgType.DST]
            item = None
            if itemHolder == self.name:
                logger.debug("Initial object holder from Server's SET")
                item = rxMsg[MsgType.DATA]

            if self.prMod is None:
                self.prMod = PathReversal(itemHolder, item, logger)
            else:
                self.prMod.set(itemHolder, item, logger)

        if len(msg) > 1:
            payload = msg[1] if isinstance(msg[1], bytes) else msg[1].encode()
            rxMsg = pickle.loads(payload)

            if rxMsg[MsgType.TYPE] == MsgType.AGENT_TEST_MSG:
                logger.debug("Received Test from Agent")
                msgOut = MsgFactory.create(MsgType.AGENT_TEST_ACK)
                self.streamCmdOut.send_multipart(
                    [self.name.encode(), msgOut],
                    callback=self.cmdOutRequestToSink)

            elif rxMsg[MsgType.TYPE] == MsgType.DATA_MOVE_MSG:
                logger.debug("Received DATA_MOVE_MSG request")
                if self.dataObjectId == rxMsg[MsgType.DATA_ID]:
                    self.ackOrForward(rxMsg, caseExisting=True)
                else:
                    self.ackOrForward(rxMsg, caseExisting=True, caseNACK=True)

            elif rxMsg[MsgType.TYPE] == MsgType.DATA_MSG:
                logger.debug("Received Data Message from Agent with ID: %s",
                             rxMsg[MsgType.DATA_ID])
                self.ackOrForward(rxMsg, caseExisting=False)

            elif rxMsg[MsgType.TYPE] == MsgType.FIFO_STATS_QUERY:
                logger.debug("Received FIFO-query-stats message from agent")
                self.streamCmdOut.send_multipart(
                    [self.name.encode(), pickle.dumps(self.fifoStats)])

            elif rxMsg[MsgType.TYPE] == MsgType.PR_GET_HUNGRY:
                logger.debug("Received Message from server to get hungry")
                txMsg, toWhom = self.prMod.becomeHungry(self.name)
                self.ackAgent()
                if txMsg is not False:
                    logger.debug("[PR-LOG] Sending PR-Request to: %s", toWhom)
                    self.peerCltStream.send_multipart([self.name.encode(), txMsg])

            elif rxMsg[MsgType.TYPE] == MsgType.PR_STATUS_ACK:
                logger.debug("Incoming status ACK")
                self.statusQueue.put(rxMsg)

    def cmdOutRequestToSink(self, msg, status):
        logger.debug("[RequestToSink] %s", msg)

    def cmdOutReplyFromSink(self, msg):
        logger.debug("[ReplyFromSink] %s", msg)

    def runFifoNetWorker(self, netName, pubAgentAddr, sinkAgentAddr, neighbor):
        self.dataObject = None
        self.dataObjectId = None
        ioloop.install()
        self.prMod = None
        self.nodeIloop = ioloop.IOLoop.instance()
        self.statusQueue = Queue()

        self.fifoStats = FifoStats()

        logFname = netName.replace(":", "_")
        logFname = "logs/" + logFname
        logging.basicConfig(level=logging.DEBUG, filename=logFname)

        self.name = netName
        self.pubAgent = pubAgentAddr
        self.sinkAgent = sinkAgentAddr

        self.neighborAddr = "tcp://" + neighbor
        self.neighbor = neighbor

        logger.debug("Creating SubAgent socket")
        self.context = zmq.Context()
        self.cmdSubSock = self.context.socket(zmq.SUB)
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, netName.encode())
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'Exit')
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'ConnectToNeighbor')
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'TestConnectionToNeighbor')
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'Reset')
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'Set')
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'Echo')

        self.cmdSubSock.connect(self.pubAgent)
        self.streamCmdIn = zmqstream.ZMQStream(self.cmdSubSock)
        self.streamCmdIn.on_recv_stream(self.procAgentCmd)

        logger.debug("Creating REQ-to-Agent socket")
        # Single REQ socket for all driver communication
        self.sinkSocket = self.context.socket(zmq.REQ)
        self.sinkSocket.connect(self.sinkAgent)

        # ZMQStream wrapper for async sends
        self.streamCmdOut = zmqstream.ZMQStream(self.sinkSocket)
        self.streamCmdOut.on_send(self.cmdOutRequestToSink)

        logger.debug("Creating Local Server socket")
        self.peerSockServ = self.context.socket(zmq.REP)
        localbindAddr = "tcp://*:" + netName.split(':')[1]
        self.peerSockServ.bind(localbindAddr)
        self.peerServStream = zmqstream.ZMQStream(self.peerSockServ)
        self.peerServStream.on_recv_stream(self.procPeerRxServerMsg)
        self.peerServStream.on_send_stream(self.procPeerTxServerMsg)

        self.nodeIloop.start()


def main():
    p = argparse.ArgumentParser(description="FifoNode Stand-alone")
    p.add_argument("-n", dest="netName", action="store", default=None,
                   help="Network name for the fifo-node")
    p.add_argument("-p", dest="pubAgentAddr", action="store", default=None,
                   help="Publisher socket address")
    p.add_argument("-s", dest="sinkAgentAddr", action="store", default=None,
                   help="Sink agent Address")
    p.add_argument("--next", dest="neighbor", action="store", default=None,
                   help="Neighbor address ")
    args = p.parse_args()

    node = FifoNode()
    node.runFifoNetWorker(args.netName, args.pubAgentAddr,
                          args.sinkAgentAddr, args.neighbor)

if __name__ == "__main__":
    main()
