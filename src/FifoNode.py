import zmq
import logging
import json
from zmq.eventloop import ioloop
from zmq.eventloop import zmqstream
from Message import MsgType
from Message import MsgFactory
from copy import deepcopy



class FifoStats(object):
    
    def __init__(self):
        self.tx = {}
        self.rx = {}
        self.n = 0
        self.conStatus = {}
        

class FifoNode(object):
    
    def __init__(self):
        self.fifoStats = None
        self.nodeIloop = None

    def procPeerServerMsg(self, stream, msg):
        logging.debug("Received message...")
        
        if len(msg) > 1:
            msgIncoming = json.loads(msg[1])
            
            if msgIncoming[MsgType.TYPE] == MsgType.KEEP_ALIVE:
                logging.debug('Received KEEP_ALIVE message from:\t'+str(msg[0]))
                logging.debug('Sending a KEEP_ALIVE_ACK message to:\t'+str(msg[0]))
                msgOut = MsgFactory.create(MsgType.KEEP_ALIVE_ACK)
                stream.send_multipart([self.name, msgOut])
        
            if msgIncoming[MsgType.TYPE] == MsgType.DATA_MSG:
                
                logging.debug('Received DATA_MSG message from:\t'+str(msg[0]))
                dataId = msgIncoming[MsgType.DATA_ID]
                msgOutDatAck = MsgFactory.create(MsgType.DATA_ACK, None, None, dataId)
                logging.debug('Sending DATA_ACK message to:\t'+str(msg[0]))
                stream.send_multipart(self.name, msgOutDatAck)
                
                if msgIncoming[MsgType.DST] == self.name:
                    
                    self.dataObject = deepcopy(msgIncoming[MsgType.DATA])
                    self.dataObjectId = msgIncoming[MsgType.DATA_ID]
                    logging.debug('DATA_MSG destination reached with id:'+
                                  str(self.dataObjectId))
                    logging.debug('Sending DATA_ACK to Agent')
                    self.streamCmdOut.send_multipart([self.name, msgOutDatAck])
                
                else:
                    logging.debug("Incoming DATA_MSG needs forwarding")
                    logging.debug("Forwarding DATA_MSG("+str(dataId)+") to neighbor:\t"+
                                  str(self.neighbor))
                    self.peerCltStream.send_multipart([self.name, msg[1]])
                    
        
    
    def procPeerClientMsg(self, msg):
        
        if len(msg) > 1:
            msgIncoming = json.loads(msg[1])
            
            if msgIncoming[MsgType.Type] == MsgType.KEEP_ALIVE_ACK:
                logging.debug("Received KEEP_ALIVE_ACK from:\t"+str(msg[0]))
                    
            if msgIncoming[MsgType.Type] == MsgType.DATA_ACK:
                logging.debug("Received DATA_MSG_ACK from:\t"+
                            str(msg[1])+"\t for:\t"+str(msgIncoming[MsgType.DATA_ID]))
            
        
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
            msgOut = MsgFactory.create(MsgType.KEEP_ALIVE,
                                       self.neighbor)
            self.peerSockClt.send_multipart([self.name, msgOut])   
        
        
        if len(msg) > 1:
            msgIncoming = json.loads(msg[1])
            
            if msgIncoming[MsgType.TYPE] == MsgType.AGENT_TEST_MSG:
                logging.debug('Received Test from Agent')
                msgOut = MsgFactory.create(MsgType.AGENT_TETS_ACK)
                self.streamCmdOut.send_multipart([self.name, msgOut])
                
            elif msgIncoming[MsgType.TYPE] == MsgType.DATA_MSG:
                logging.debug('Received Data Message from Agent with ID:\t'+
                              str(msgIncoming[MsgType.DATA_ID]))
                
                if msgIncoming[MsgType.DST] == self.name:
                    msgOut = MsgFactory.create(MsgType.DATA_ACK)
                    self.streamCmdOut.send_multipart([self.name, msgOut])
                else:
                    msgOut = [self.name, msg[1]]
                    self.peerSockClt.send_multipart(msgOut)
        
        
        
        logging.debug("Exiting process Command")
        return
        
    
    def runFifoNetWorker(self, netName, pubAgentAddr, sinkAgentAddr, neighbor):
    
        ioloop.install()
        self.nodeIloop = ioloop.IOLoop.instance()
        self.fifoStats = FifoStats()
        
        logFname = netName.replace(":", "_")
        logFname = "logs/"+logFname
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
        
    
    
    
              
            
    