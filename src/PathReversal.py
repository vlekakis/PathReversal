from Message import MsgType
from Message import MsgFactory

class PRStatus(object):
    THINKING = 0
    HUNGRY = 1
    EATING = 2

class PRNext(object):
    FORWARD = 3
    RX_OBJ = 4
    TX_OBJ = 5

class PathReversal(object):
    
    def __init__(self, itemHolder, item):
        self.set(itemHolder, item)
    
    def becomeHungry(self, issuerNetName):
        if self.status == PRStatus.THINKING:
            self.status = PRStatus.HUNGRY
            prReq = MsgFactory.create(MsgType.PR_REQ, 
                                    dst=self.last, src=issuerNetName)
            return prReq
        return False
    
    def becomeThinking(self):
        if self.status == PRStatus.EATING and \
            self.next != None:
            
            self.status = PRStatus.THINKING
            self.next = None
            self.data = None
            
            return True
        return False
    
    
    def reset(self):
        self.data = None
        self.next = None
        self.last = None
        self.status = PRStatus.THINKING
    
    def set(self, itemHolder, item):
        if item != None:
            self.last = None
            self.next = None
            self.data = item
            self.status = PRStatus.EATING
        else:
            self.last = itemHolder
            self.next = None
            self.object = None
            self.status = PRStatus.THINKING
            
    
    def becomeEating(self):
        self.status = PRStatus.EATING
        
    
    def recv(self, forwarderNetName,msg):
        
        toNode = None
        if self.object == None:
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
                
        #elif msg[MsgType.TYPE] == MsgType.PR_OBJ:
        #    self.becomeEating()
        #    return (False, None)