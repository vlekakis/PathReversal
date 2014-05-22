import threading
import zmq

class AgentSinkServer(threading.Thread):

    def __init__(self, messageQueue, sinkSock, bindAddr):
        super(AgentSinkServer, self).__init__()
        self.msgQueue = messageQueue
        self.stopRequest = threading.Event()
        self.sinkSock = sinkSock
        self.bindAddr = bindAddr
        self.sinkSock.bind(self.bindAddr)
        
         
    def run(self):
        poller = zmq.Poller()
        poller.register(self.sinkSock, zmq.POLLIN)
        
        while not self.stopRequest.isSet():
            try:
                socks = dict(poller.poll(1000))
            except KeyboardInterrupt:
                break
            if self.sinkSock in socks:
                msg = self.sinkSock.recv_multipart()
                self.msgQueue.put(msg, False)
        
    def join(self, timeout=0):
        self.stopRequest.set()
        super(AgentSinkServer, self).join(timeout)
        