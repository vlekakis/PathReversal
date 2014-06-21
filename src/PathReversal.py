

import cPickle
from Message import MsgType
from Message import MsgFactory

class PRStatusUpdate(object):
    NEXT = "next"
    LAST =  "last"
    STATUS = "status"
    SEQ = "SEQ"
    SATISFY = "SATISFY"

    @staticmethod
    def createStatusUpdate(mtype, seqNum, status, next, last, satisfy):
        statusUpdate = {}
        statusUpdate[MsgType.TYPE] = mtype
        statusUpdate[PRStatusUpdate.SEQ] = seqNum
        statusUpdate[PRStatusUpdate.STATUS] = status
        statusUpdate[PRStatusUpdate.NEXT] = next
        statusUpdate[PRStatusUpdate.LAST] = last
        statusUpdate[PRStatusUpdate.SATISFY] = satisfy
        statusUpdate = cPickle.dumps(statusUpdate)
        return statusUpdate
    
    @staticmethod
    def createStatusACKMessage(seqNum):
        statusAck = {}
        statusAck[MsgType.TYPE] = MsgType.PR_STATUS_ACK
        statusAck[PRStatusUpdate.SEQ] = seqNum
        statusAck = cPickle.dumps(statusAck)
        return statusAck
    

class PRStatus(object):
    THINKING = 0
    HUNGRY = 1
    EATING = 2

class PRNext(object):
    FORWARD = 3
    RX_OBJ = 4
    TX_OBJ = 5

class PathReversal(object):
    
    def __init__(self, itemHolder, item, logger):
        self.set(itemHolder, item, logger)
        self.seq = 0
    
    def becomeHungry(self, issuerNetName):
        if self.status == PRStatus.THINKING:
            self.status = PRStatus.HUNGRY
            prReq = MsgFactory.create(MsgType.PR_REQ, 
                                    dst=self.last, src=issuerNetName)
            self.logger.debug("\t[PR-LOG] Status: HUNGRY")
            return prReq,self.last
        return False
    
    def becomeThinking(self):
        if self.status == PRStatus.EATING and \
            self.next != None:
            
            self.status = PRStatus.THINKING
            self.next = None
            self.data = None
            self.logger.debug("\t[PR-LOG] Status: THINKING")
            return True
        return False
    
    def getStatus(self):
        status = (self.seq, self.status, self.next, self.last)
        self.seq+=1
        return status
    
    def reset(self):
        self.data = None
        self.next = None
        self.last = None
        self.status = PRStatus.THINKING
    
    def set(self, itemHolder, item, logger):
        self.logger = logger
        if item != None:
            self.last = None
            self.next = None
            self.becomeEating(item)
        else:
            self.last = itemHolder
            self.next = None
            self.data = None
            self.status = PRStatus.THINKING
            
    
    def becomeEating(self, data):
        self.logger.debug("\t[PR-LOG] Status: EATING data:"+str(data))
        self.status = PRStatus.EATING
        self.data = data
    
    def recv(self, forwarderNetName,msg):
        
        toNode = None
        if self.data == None:
            if self.last != None:
                toNode = self.last
                self.last = msg[MsgType.SOURCE]
            else:
                toNode = self.next
                self.last = msg[MsgType.SOURCE]
                self.next = msg[MsgType.SOURCE]
                
            return (PRNext.FORWARD, toNode)
        
        else:
            toTx = (PRNext.TX_OBJ, self.data)
            self.next = msg[MsgType.SOURCE]
            self.last = msg[MsgType.SOURCE]
            assert self.becomeThinking()
            
            return toTx
        