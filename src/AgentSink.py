import threading
import zmq

class AgentSinkServer(threading.Thread):

    def __init__(self, writeQueue, readQueue, sinkSock, bindAddr):
        super(AgentSinkServer, self).__init__()
        self.writeQueue = writeQueue
        self.readQueue = readQueue
        
        self.stopRequest = threading.Event()
        self.sinkSock = sinkSock
        self.bindAddr = bindAddr
        self.sinkSock.bind(self.bindAddr)
        
    def run(self):
        while not self.stopRequest.is_set():
            msgIn = self.sinkSock.recv_multipart()
            self.writeQueue.put(msgIn,False)
            
            msgOut = self.readQueue.get(True)
            self.readQueue.task_done()
            self.sinkSock.send_multipart(msgOut)
        
         
    def join(self, timeout=0):
        self.stopRequest.set()
        super(AgentSinkServer, self).join(timeout)
        