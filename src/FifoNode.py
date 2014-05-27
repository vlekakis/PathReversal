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

    def peerServerReplying(self, stream, msg, status):
        logging.debug("\tPeerServerReplying to: "+str(msg[0]))
        logging.debug(str(msg))

    def procPeerServerMsg(self, stream, msg):
        logging.debug("\tReceived message...")
        
        if len(msg) > 1:
            msgIncoming = json.loads(msg[1])
            
            if msgIncoming[MsgType.TYPE] == MsgType.KEEP_ALIVE:
                logging.debug('\tReceived KEEP_ALIVE message from:\t'+str(msg[0]))
                logging.debug('\tSending a KEEP_ALIVE_ACK message to:\t'+str(msg[0]))
                msgOut = MsgFactory.create(MsgType.KEEP_ALIVE_ACK, msg[0])
                try:
                    stream.send_multipart([self.name, msgOut])
                except TypeError as e:
                    logging.debug(e.message())
            if msgIncoming[MsgType.TYPE] == MsgType.DATA_MSG:
                
                
                did = msgIncoming[MsgType.DATA_ID]
                logging.debug('\tReceived DATA_MSG message from:\t'+str(msg[0])+
                              " with id\t"+did)
                msgOut = MsgFactory.create(MsgType.DATA_ACK, None, None, did, None)
                logging.debug('\tSending DATA_ACK message to:\t'+str(msg[0]))
                if stream.closed():
                    logging.debug("STREAM CLOSED")
                try:
                    stream.send_multipart([self.name, msgOut])
                except ValueError as e:
                    logging.debug(e.message)
                
                if msgIncoming[MsgType.DST] == self.name:
                    
                    self.dataObject = deepcopy(msgIncoming[MsgType.DATA])
                    self.dataObjectId = msgIncoming[MsgType.DATA_ID]
                    logging.debug('\tDATA_MSG destination reached with id:'+
                                   str(self.dataObjectId))
                    logging.debug('\tSending DATA_ACK to Agent')
                    msgOutDatAck = MsgFactory.create(MsgType.DATA_ACK, None, None, did, None)
                    self.streamCmdOut.send_multipart([self.name, msgOutDatAck])
                        
                else:
                    logging.debug("\tIncoming DATA_MSG needs forwarding")
                    logging.debug("\tForwarding DATA_MSG("+str(did)+") to neighbor:\t"+
                                  str(self.neighbor))
                    self.peerCltStream.send_multipart([self.name, msg[1]])
                    
    
    def procPeerClientMsg(self, msg):
        
        if len(msg) > 1:
            msgIncoming = json.loads(msg[1])
            
            if msgIncoming[MsgType.TYPE] == MsgType.KEEP_ALIVE_ACK:
                logging.debug("\tReceived KEEP_ALIVE_ACK from:\t"+str(msg[0]))
                    
            if msgIncoming[MsgType.TYPE] == MsgType.DATA_ACK:
                logging.debug("\tReceived DATA_MSG_ACK from:\t"+
                            str(msg[1])+"\t for:\t"+str(msgIncoming[MsgType.DATA_ID]))
            
   
    def ackOrForward(self, msgIn, caseExisting=False, caseNACK=False):
        
        did = msgIn[MsgType.DATA_ID]
        if msgIn[MsgType.DST] == self.name or caseNACK == True:
            if caseNACK == False:
                msgOut = MsgFactory.create(MsgType.DATA_ACK, dataId=did)
                logging.debug("\t Object\t"+str(did)+" is home. Sending ACK to the agent")
            else:
                msgOut = MsgFactory.create(MsgType.DATA_NACK, dataId=did)
                logging.debug("\t Object\t"+str(did)+" is NOT home. Sending NACK to the agent")
                
            self.streamCmdOut.send_multipart([self.name, msgOut])
            
            
        else:
            if caseExisting == True:
                msgIn[MsgType.TYPE] = MsgType.DATA_MSG
                
            msgOut = [self.name, json.dumps(msgIn)]
            logging.debug("\tIncoming DATA_MSG needs forwarding")
            logging.debug("\tForwarding DATA_MSG("+str(did)+") to neighbor:\t"+
                          str(self.neighbor))
            self.peerSockClt.send_multipart(msgOut)
            
            
                    
   
    def procAgentCmd(self, stream, msg):
        
        if msg[0] == 'Exit':
            logging.debug("Received exit")
            stream.stop_on_recv()
            self.nodeIloop.stop()
        
        if msg[0] == 'ConnectToNeighbor':
            logging.debug("\tConnectingToNeighbor  CMD arrived")
            self.peerSockClt = self.context.socket(zmq.REQ)
            self.peerSockClt.connect(self.neighborAddr)
            self.peerCltStream = zmqstream.ZMQStream(self.peerSockClt)
            self.peerCltStream.on_recv(self.procPeerClientMsg)
       
        if  msg[0] == 'TestConnectionToNeighbor':
            logging.debug('\tTestConnection With the Peer-Neighbor')
            msgOut = MsgFactory.create(MsgType.KEEP_ALIVE,
                                       self.neighbor)
            self.peerSockClt.send_multipart([self.name, msgOut])   
        
        
        if len(msg) > 1:
            msgIncoming = json.loads(msg[1])
            
            if msgIncoming[MsgType.TYPE] == MsgType.AGENT_TEST_MSG:
                logging.debug('\tReceived Test from Agent')
                msgOut = MsgFactory.create(MsgType.AGENT_TETS_ACK)
                self.streamCmdOut.send_multipart([self.name, msgOut])
            
            if msgIncoming[MsgType.TYPE] == MsgType.DATA_MOVE_MSG:
                logging.debug("Received DATA_MOVE_MSG request")
                if self.dataObjectId == msgIncoming[MsgType.DATA_ID]:
                    self.ackOrForward(msgIncoming, caseExisting=True)
                else:
                    self.ackOrForward(msgIncoming, caseExisting=True, caseNACK=True)
                    
            elif msgIncoming[MsgType.TYPE] == MsgType.DATA_MSG:
                logging.debug('\tReceived Data Message from Agent with ID:\t'+
                              str(msgIncoming[MsgType.DATA_ID]))
                self.ackOrForward(msgIncoming, caseExisting=False)
        
        
        logging.debug("\tExiting process Command")
        return
        
    
    def runFifoNetWorker(self, netName, pubAgentAddr, sinkAgentAddr, neighbor):
    
        self.dataObject = None
        self.dataObjectId = None
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
    
        logging.debug("\tCreating SubAgent socket")
        self.context = zmq.Context()
        self.cmdSubSock = self.context.socket(zmq.SUB)
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, netName)
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'Exit')
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'ConnectToNeighbor')
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'TestConnectionToNeighbor')
        self.cmdSubSock.connect(self.pubAgent)
        self.streamCmdStream = zmqstream.ZMQStream(self.cmdSubSock)
        self.streamCmdStream.on_recv_stream(self.procAgentCmd)
        
        
        logging.debug("\tCreating PUSH-to-Agent socket")
        self.cmdPushSock = self.context.socket(zmq.PUSH)
        self.cmdPushSock.connect(self.sinkAgent)
        self.streamCmdOut = zmqstream.ZMQStream(self.cmdPushSock)

        
        logging.debug("\tCreating Local Server socket")
        self.peerSockServ = self.context.socket(zmq.REP)
        localbindAddr = "tcp://*:"+netName.split(':')[1]
        self.peerSockServ.bind(localbindAddr)
        self.peerServStream = zmqstream.ZMQStream(self.peerSockServ)
        self.peerServStream.on_recv_stream(self.procPeerServerMsg)
        self.peerServStream.on_send_stream(self.peerServerReplying)
        
        
        self.nodeIloop.start()
        
    
    