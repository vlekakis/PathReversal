"""Agent Sink Server — bridges ZMQ REQ/REP between nodes and the driver."""

import threading
import zmq


class AgentSinkServer(threading.Thread):

    def __init__(self, writeQueue, readQueue, sinkSock, bindAddr):
        super().__init__()
        self.writeQueue = writeQueue
        self.readQueue = readQueue
        self.stopRequest = threading.Event()
        self.sinkSock = sinkSock
        self.bindAddr = bindAddr
        self.sinkSock.bind(self.bindAddr)
        self.daemon = True

    def run(self):
        while not self.stopRequest.is_set():
            try:
                msgIn = self.sinkSock.recv_multipart(flags=zmq.NOBLOCK)
                self.writeQueue.put(msgIn, False)

                msgOut = self.readQueue.get(True, timeout=5)
                self.readQueue.task_done()
                self.sinkSock.send_multipart(msgOut)
            except zmq.Again:
                # No message available, check stop flag
                self.stopRequest.wait(0.1)
            except Exception:
                if self.stopRequest.is_set():
                    break

    def join(self, timeout=5):
        self.stopRequest.set()
        super().join(timeout)
