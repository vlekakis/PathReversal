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

    def procPeerServerMsg(self, stream, msg):
        logging.debug("Received message...")
        
        if len(msg) > 1 and msg[1] == 'Hello':
            logging.debug('Sending ACK to a Hello message')
            stream.send_multipart([msg[0], 'ACK'])
    
    def procPeerClientMsg(self, msg):
        
        if len(msg) > 1 and msg[1] == 'ACK':
            logging.debug("Received ACK for the Hello Message")

    def procAgentCmd(self, stream, msg):
        
        logging.debug("\tIncoming msg\t"+str(msg))
        if msg[0] == 'Exit':
            logging.debug("Received exit")
            stream.stop_on_recv()
            self.nodeIloop.stop()
        
        if msg[0] == 'ConnectToNeighbor':
            logging.debug("ConnectingToNeighbor  CMD arrived")
            self.peerSockClt = self.context.socket(zmq.REQ)
            self.peerSockClt.connect(self.neighborAddr)
            self.peerCltStream = zmqstream.ZMQStream(self.peerSockClt)
            self.peerCltStream.on_recv(self.procPeerClientMsg)
       
        if  msg[0] == 'TestConnectionToNeighbor':
            logging.debug('TestConnection With the Peer-Neighbor')
            self.peerSockClt.send_multipart([self.name, 'Hello'])
    
       
        
        if len(msg) > 1 and msg[1] == 'Test':
            logging.debug('Received Test from Agent')
            self.streamCmdOut.send_multipart([self.name, 'ACK'])
        
       
        
        
        logging.debug("Exiting process Command")
        return
        
    
    def runFifoNetWorker(self, netName, pubAgentAddr, sinkAgentAddr, neighbor):
    
        logFname = netName.replace(":", "_")
        logging.basicConfig(filename=logFname,level=logging.DEBUG)
        
        self.name = netName
        self.pubAgent = pubAgentAddr
        self.sinkAgent = sinkAgentAddr
        self.neighborAddr = "tcp://"+neighbor
        self.neighbor = neighbor
    
        logging.debug("Creating SubAgent socket")
        self.context = zmq.Context()
        self.cmdSubSock = self.context.socket(zmq.SUB)
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, netName)
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'Exit')
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'ConnectToNeighbor')
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'TestConnectionToNeighbor')
        self.cmdSubSock.connect(self.pubAgent)
        self.streamCmdStream = zmqstream.ZMQStream(self.cmdSubSock)
        self.streamCmdStream.on_recv_stream(self.procAgentCmd)
        
        
        logging.debug("Creating PUSH-to-Agent socket")
        self.cmdPushSock = self.context.socket(zmq.PUSH)
        self.cmdPushSock.connect(self.sinkAgent)
        self.streamCmdOut = zmqstream.ZMQStream(self.cmdPushSock)
        
        
        logging.debug("Creating Local Server socket")
        self.peerSockServ = self.context.socket(zmq.REP)
        localbindAddr = "tcp://*:"+netName.split(':')[1]
        self.peerSockServ.bind(localbindAddr)
        self.peerServStream = zmqstream.ZMQStream(self.peerSockServ)
        self.peerServStream.on_recv_stream(self.procPeerServerMsg)
        
        self.nodeIloop.start()
        
    
    
    
              
            
    