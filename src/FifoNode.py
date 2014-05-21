import zmq
from zmq.eventloop import ioloop
from zmq.eventloop import zmqstream
from sys import exit
from sys import argv
import logging
import json

from time import sleep

NodeIloop = None

logging.basicConfig(filename="process.cmd",level=logging.DEBUG)
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

    def processCmd(self, stream , msg):
    
        if msg[0] == 'Exit':
            logging.debug("Received exit")
            stream.stop_on_recv()
            self.nodeIloop.stop()
        
        logging.debug("Exiting process cmd")
        return
        
    
    def runFifoNetWorker(self, netName, ctrlAddress):
    
    
        context = zmq.Context()
        netCtrlRxSock = context.socket(zmq.SUB)
        netCtrlRxSock.setsockopt(zmq.SUBSCRIBE, netName)
        netCtrlRxSock.setsockopt(zmq.SUBSCRIBE, b'Exit')
        netCtrlRxSock.connect(ctrlAddress)
        streamCtrl = zmqstream.ZMQStream(netCtrlRxSock)
    
        streamCtrl.on_recv_stream(self.processCmd)
        self.nodeIloop.start()
        
    
    
    
              
            
    