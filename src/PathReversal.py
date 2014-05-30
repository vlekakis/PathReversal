from Message import MsgType
from Message import MsgFactory

class PRStatus(object):
    THINKING = 0
    HUNGRY = 1
    EATING = 2


class PathReversal(object):
    
    def __init__(self, objectHolder, myAddr):
        
        self.msgSeenSinceLastHungry = 0
        if myAddr == objectHolder:
            self.last = None
            self.next = None
            self.status = PRStatus.EATING
        else:
            self.last = objectHolder
            self.next = None
            self.status = PRStatus.THINKING 
        
        self.object  = None
    
    
    
    def becomeHungry(self):
        if self.status == PRStatus.THINKING:
            self.status = PRStatus.HUNGRY
            self.msgSeenSinceLastHungry = 0
            return True
        return False
    
    def becomeThinking(self):
        if self.status == PRStatus.EATING and \
            self.next != None:
            self.status = PRStatus.THINKING
            return True
        return False
    
    
    def becomeEating(self):
        self.status = PRStatus.EATING
    
    def recv(self, msg):
        if msg[MsgType.TYPE] == MsgType.PR_REQ:
            if self.last != None:
        elif msg[MsgType.TYPE] == MsgType.PR_OBJ:
            self.becomeEating()