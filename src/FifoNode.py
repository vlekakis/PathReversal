import zmq
import logging
import cPickle
import argparse
from PathReversal import PRNext
from PathReversal import PathReversal
from PathReversal import PRStatusUpdate
from zmq.eventloop import ioloop
from zmq.eventloop import zmqstream
from Message import MsgType
from Message import MsgFactory
from copy import deepcopy
from Queue import Queue, Empty


class FifoStats(object):
    
    def __init__(self):
        self.tx = {}
        self.rx = {}
        self.n = 0
        self.conStatus = {}
        
    def repr(self):
        print self.tx
        print self.rx
        

class FifoNode(object):
    
    def __init__(self):
        self.fifoStats = None
        self.nodeIloop = None

    def getDest(self, msg):
        m = cPickle.loads(msg[1])
        return m[MsgType.DST]
    
    def updateFifoStats(self, dest, msg, tx=False, rx=False):
        key = repr((self.name, dest))
        if tx == True:
            if key not in self.fifoStats.tx.keys():
                self.fifoStats.tx[key] = []
            self.fifoStats.tx[key].append(msg)
        elif rx == True:
            if key not in self.fifoStats.rx.keys():
                self.fifoStats.rx[key] = []
            self.fifoStats.rx[key].append(msg)
            

    def procPeerTxServerMsg(self, stream, msg, status):
        
        logging.debug(str(msg))
        dst = self.getDest(msg)
        logging.debug("\tServer replying to: "+str(dst))
        logging.debug(str(msg))
        self.updateFifoStats(dst, msg, tx=True)


    def ackAgent(self, satisfy=False):

        
        prNodeStatus = self.prMod.getStatus()
        prNodeStatusMsg = PRStatusUpdate.createStatusUpdate(MsgType.PR_STATUS_UPDATE,
                                                           prNodeStatus[0],
                                                           prNodeStatus[1],
                                                           prNodeStatus[2],
                                                           prNodeStatus[3],
                                                           satisfy)
        logging.debug("\t[PR-LOG] ACK-AGENT3")
        logging.debug("\t[PR-LOG] Sending Status update to Agent ("+str(prNodeStatus[0])+")")
        
        
        #self.streamCmdOut.send_multipart([self.name, prNodeStatusMsg], callback=self.cmdOutRequestToSink)
        #self.streamCmdOut.flush()
        
        self.stupidVerificationSocket.send_multipart([self.name, prNodeStatusMsg])
        updateAck = self.stupidVerificationSocket.recv_multipart()
        logging.debug( str(updateAck))
        updateAck = cPickle.loads(updateAck[1])
        logging.debug( str(updateAck))
        assert prNodeStatus[0] == updateAck[PRStatusUpdate.SEQ]

        logging.debug("AfterQueue")
        logging.debug("Update ACK \t"+str(updateAck))
        logging.debug("PrNodeStatus \t"+str(prNodeStatus))
        
    
    def procPeerRxPathReversalMsg(self, stream, rxMsg, sender, pureMsg):
        
        logging.debug("\t[PR-LOG] Sending PR_ACK to:"+str(sender))
        ackMsg = MsgFactory.create(MsgType.PR_ACK, dst=sender)
        stream.send_multipart([self.name, ackMsg])

        
        if rxMsg[MsgType.DST] != self.name:
            if rxMsg[MsgType.TYPE] == MsgType.PR_REQ:
                logging.debug("\t[PR-LOG] Forwarding PR_REQ to:"+self.neighbor)
            elif rxMsg[MsgType.TYPE] == MsgType.PR_OBJ:
                logging.debug("\t[PR-LOG] Forwarding PR_OBJ to:"+self.neighbor)
            self.peerCltStream.send_multipart([self.name, pureMsg])
                
        elif rxMsg[MsgType.DST] == self.name:
            
            if rxMsg[MsgType.TYPE] == MsgType.PR_REQ:
            
                txMsg = None
                logging.debug("\t[PR-LOG] PR_REQ reached DST")
                action, actionArg = self.prMod.recv(self.name, rxMsg)
                
                self.ackAgent()
                                                                   
                if action == PRNext.FORWARD:
                    logging.debug("\t[PR-LOG] New Request FWD to:"+actionArg)
                    txMsg = MsgFactory.create(MsgType.PR_REQ, 
                                          dst=actionArg, 
                                          src=rxMsg[MsgType.SOURCE])
                    
                elif action == PRNext.TX_OBJ:
                    logging.debug("\t[PR-LOG] Serving OBJECT to:"+rxMsg[MsgType.SOURCE])
                    txMsg = MsgFactory.create(MsgType.PR_OBJ, 
                                          dst=rxMsg[MsgType.SOURCE],
                                          data=actionArg,  
                                          src=self.name)
                                            
                self.peerCltStream.send_multipart([self.name, txMsg])
                
            elif rxMsg[MsgType.TYPE] == MsgType.PR_OBJ:
                logging.debug("\t[PR-LOG] Received OBJECT from:"+rxMsg[MsgType.SOURCE])
                self.prMod.becomeEating(rxMsg[MsgType.DATA])
                
                logging.debug("\t[PR-LOG] Inform the server about EATING")
                self.ackAgent(satisfy=True)

            
    def procPeerRxServerMsg(self, stream, msg):
        logging.debug("\tReceived message...")
        rxMsg = cPickle.loads(msg[1])
        logging.debug("GENERIC:\t"+str(rxMsg))
            
        if rxMsg[MsgType.TYPE] == MsgType.PR_REQ or \
            rxMsg[MsgType.TYPE] == MsgType.PR_OBJ:
            self.procPeerRxPathReversalMsg(stream, rxMsg, msg[0], msg[1])            
            
                
        if rxMsg[MsgType.TYPE] == MsgType.KEEP_ALIVE:
            logging.debug('\tReceived KEEP_ALIVE message from:\t'+str(msg[0]))
            logging.debug('\tSending a KEEP_ALIVE_ACK message to:\t'+str(msg[0]))
            msgOut = MsgFactory.create(MsgType.KEEP_ALIVE_ACK, msg[0])
            try:
                stream.send_multipart([self.name, msgOut])
            except TypeError as e:
                logging.debug(e.message())

        if rxMsg[MsgType.TYPE] == MsgType.DATA_MSG:    
            did = rxMsg[MsgType.DATA_ID]
            logging.debug('\tReceived DATA_MSG message from:\t'+str(msg[0])+
                          " with id\t"+did)
            msgOut = MsgFactory.create(MsgType.DATA_ACK, None, None, did, None)
            logging.debug('\tSending DATA_ACK message to:\t'+str(msg[0]))
            stream.send_multipart([self.name, msgOut])
           
        
            if rxMsg[MsgType.DST] == self.name:    
                self.dataObject = deepcopy(rxMsg[MsgType.DATA])
                self.dataObjectId = rxMsg[MsgType.DATA_ID]
                logging.debug('\t[PR-LOG]DATA_MSG destination reached with id:'+
                               str(self.dataObjectId))
                logging.debug('\tSending DATA_ACK to Agent')
                msgOutDatAck = MsgFactory.create(MsgType.DATA_ACK, None, None, did, None)
                self.streamCmdOut.send_multipart([self.name, msgOutDatAck])
                    
            else:
                logging.debug("\tIncoming DATA_MSG needs forwarding")
                logging.debug("\tForwarding DATA_MSG("+str(did)+") to neighbor:\t"+
                              str(self.neighbor))
                self.peerCltStream.send_multipart([self.name, msg[1]])
                    
                    
    def procPeerTxClientMsg(self, msg, status):
        dst = self.getDest(msg)
        logging.debug("Client sending: "+str(msg))
        self.updateFifoStats(dst, msg, tx=True)
    
    def procPeerRxClientMsg(self, msg):
        
        if len(msg) > 1:
            rxMsg = cPickle.loads(msg[1])
            self.updateFifoStats(msg[0], msg, rx=True)
            
            if rxMsg[MsgType.TYPE] == MsgType.KEEP_ALIVE_ACK:
                logging.debug("\tReceived KEEP_ALIVE_ACK from:\t"+str(msg[0]))
                    
            if rxMsg[MsgType.TYPE] == MsgType.DATA_ACK:
                logging.debug("\tReceived DATA_MSG_ACK from:\t"+
                            str(msg[1])+"\t for:\t"+str(rxMsg[MsgType.DATA_ID]))
   
            if rxMsg[MsgType.TYPE] == MsgType.PR_ACK:
                logging.debug("\t[PR-LOG] Received ACK for PR-REQ/PR_OBJ")
   
    def ackOrForward(self, msgIn, caseExisting=False, caseNACK=False):
        
        did = msgIn[MsgType.DATA_ID]
        if msgIn[MsgType.DST] == self.name or caseNACK == True:
            if caseNACK == False:
                msgOut = MsgFactory.create(MsgType.DATA_ACK, dataId=did)
                logging.debug("\tObject\t"+str(did)+" is home. Sending ACK to the agent")
            else:
                msgOut = MsgFactory.create(MsgType.DATA_NACK, dataId=did)
                logging.debug("\t Object\t"+str(did)+" is NOT home. Sending NACK to the agent")
                
            self.streamCmdOut.send_multipart([self.name, msgOut])
            self.streamCmdOut.flush()
            
            
        else:
            if caseExisting == True:
                msgIn[MsgType.TYPE] = MsgType.DATA_MSG
                
            msgOut = [self.name, cPickle.dumps(msgIn)]
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
            self.peerCltStream.on_recv(self.procPeerRxClientMsg)
            self.peerCltStream.on_send(self.procPeerTxClientMsg)
       
        if  msg[0] == 'TestConnectionToNeighbor':
            logging.debug('\tTestConnection With the Peer-Neighbor')
            msgOut = MsgFactory.create(MsgType.KEEP_ALIVE,
                                       self.neighbor)
            self.peerSockClt.send_multipart([self.name, msgOut])   
        
        if msg[0] == 'Reset':
            logging.debug('\tServer send RESET message')
            self.prMod.reset()
        
        if msg[0] == 'Echo':
            logging.debug("\t Server sends echo message: "+(str(msg[1])))
        
        if msg[0] == 'Set':
            logging.debug('\tServer send SET message')
            rxMsg = cPickle.loads(msg[1])
            itemHolder = rxMsg[MsgType.DST]
            item = None
            if itemHolder == self.name:
                logging.debug("\tInitial object holder from Server's SET")
                item = rxMsg[MsgType.DATA]
                
            if self.prMod == None:
                self.prMod = PathReversal(itemHolder, item, logging)
            else:
                self.prMod.set(itemHolder,item, logging)
        
        if len(msg) > 1:
            rxMsg = cPickle.loads(msg[1])
            
            if rxMsg[MsgType.TYPE] == MsgType.AGENT_TEST_MSG:
                logging.debug('\tReceived Test from Agent')
                msgOut = MsgFactory.create(MsgType.AGENT_TETS_ACK)
                self.streamCmdOut.send_multipart([self.name, msgOut], callback=self.cmdOutRequestToSink)
            
            elif rxMsg[MsgType.TYPE] == MsgType.DATA_MOVE_MSG:
                logging.debug("Received DATA_MOVE_MSG request")
                if self.dataObjectId == rxMsg[MsgType.DATA_ID]:
                    self.ackOrForward(rxMsg, caseExisting=True)
                else:
                    self.ackOrForward(rxMsg, caseExisting=True, caseNACK=True)
                    
            elif rxMsg[MsgType.TYPE] == MsgType.DATA_MSG:
                logging.debug('\t[PR-LOG]Received Data Message from Agent with ID:\t'+
                              str(rxMsg[MsgType.DATA_ID]))
                self.ackOrForward(rxMsg, caseExisting=False)
            
            elif rxMsg[MsgType.TYPE] == MsgType.FIFO_STATS_QUERY:
                logging.debug("\tReceived FIFO-query-stats message from agent")
                self.streamCmdOut.send_multipart([self.name, cPickle.dumps(self.fifoStats)])
            
            elif rxMsg[MsgType.TYPE] == MsgType.PR_GET_HUNGRY:
                logging.debug("\tReceived Message from server to get hungry")
                txMsg, toWhom = self.prMod.becomeHungry(self.name)
                self.ackAgent()
                if txMsg != False:
                    logging.debug("\t[PR-LOG] Sending PR-Request to: "+toWhom)
                    self.peerCltStream.send_multipart([self.name, txMsg])
            
            elif rxMsg[MsgType.TYPE] == MsgType.PR_STATUS_ACK:
                logging.debug("======Incoming status ACK")
                
                print "======Incoming status ACK"
                self.statusQueue.put(rxMsg)
            
                    
        return
        
    def cmdOutRequestToSink(self, msg, status):
        logging.debug("[RequestToSink]\t"+str(msg))
    
    def cmdOutReplyFromSink(self, msg):
        logging.debug("[ReplyFromSink]\t"+str(msg))
        
    def runFifoNetWorker(self, netName, pubAgentAddr, sinkAgentAddr, neighbor):
    
        self.dataObject = None
        self.dataObjectId = None
        ioloop.install()
        self.prMod = None
        self.nodeIloop = ioloop.IOLoop.instance()
        self.statusQueue = Queue()
        
        self.fifoStats = FifoStats()
        
        logFname = netName.replace(":", "_")
        logFname = "logs/"+logFname
        logging.basicConfig(level=logging.DEBUG, filename=logFname)
        
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
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'Reset')
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'Set')
        self.cmdSubSock.setsockopt(zmq.SUBSCRIBE, b'Echo')
        
        self.cmdSubSock.connect(self.pubAgent)
        self.streamCmdIn = zmqstream.ZMQStream(self.cmdSubSock)
        self.streamCmdIn.on_recv_stream(self.procAgentCmd)
        
        
        logging.debug("\tCreating PUSH-to-Agent socket")
        self.cmdReqRepSock = self.context.socket(zmq.REQ)
        self.cmdReqRepSock.connect(self.sinkAgent)
        self.streamCmdOut = zmqstream.ZMQStream(self.cmdReqRepSock)
        self.streamCmdOut.on_send(self.cmdOutRequestToSink)
        
        self.stupidVerificationSocket =  self.context.socket(zmq.REQ)
        self.stupidVerificationSocket.connect(self.sinkAgent)
        
        
        logging.debug("\tCreating Local Server socket")
        self.peerSockServ = self.context.socket(zmq.REP)
        localbindAddr = "tcp://*:"+netName.split(':')[1]
        self.peerSockServ.bind(localbindAddr)
        self.peerServStream = zmqstream.ZMQStream(self.peerSockServ)
        self.peerServStream.on_recv_stream(self.procPeerRxServerMsg)
        self.peerServStream.on_send_stream(self.procPeerTxServerMsg)
        
        
        self.nodeIloop.start()
        
def main():
    #netName, pubAgentAddr, sinkAgentAddr, neighbor
    
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