import zmq
from zmq.eventloop import ioloop
from zmq.eventloop import zmqstream
from sys import exit
from sys import argv
import logging
import json

from time import sleep




class FifoStats(object):
    
    def __init__(self):
        self.tx = {}
        self.rx = {}
        self.n = 0
        self.conStatus = {}
        

class FifoNode(object):
    
    def __init__(self):
        ioloop.install()
        self.fifoStats = FifoStats()
        self.nodeIloop = ioloop.IOLoop.instance()

    def processAgentCmd(self, stream, msg):
        
        logging.debug("\tIncoming msg\t"+str(msg))
        if msg[0] == 'Exit':
            logging.debug("Received exit")
            stream.stop_on_recv()
            self.nodeIloop.stop()
        
        
        if len(msg) > 1 and msg[1] == 'Test':
            logging.debug('Received Test from Agent')
            self.streamCmdOut.send_multipart([self.name, 'ACK'])
        
        logging.debug("Exiting process Command")
        return
        
    
    def runFifoNetWorker(self, netName, pubAgentAddr, sinkAgentAddr):
    
        logFname = netName.replace(":", "_")
        logging.basicConfig(filename=logFname,level=logging.DEBUG)
        
        self.name = netName
        self.pubAgent = pubAgentAddr
        self.sinkAgent = sinkAgentAddr
    
        logging.debug("Creating SubAgent socket")
        self.context = zmq.Context()
        self.cmdSubSock = self.context.socket(zmq.SUB)
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, netName)
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'Exit')
        self.cmdSubSock.connect(self.pubAgent)
        self.streamCmdIn = zmqstream.ZMQStream(self.cmdSubSock)
        self.streamCmdIn.on_recv_stream(self.processAgentCmd)
        
        
        logging.debug("Creating PUSH-to-Agent socket")
        self.cmdPushSock = self.context.socket(zmq.PUSH)
        self.cmdPushSock.connect(self.sinkAgent)
        self.streamCmdOut = zmqstream.ZMQStream(self.cmdPushSock)
        #self.streamCmdOut.on_send ???
        self.nodeIloop.start()
        
    
    
    
              
            
    