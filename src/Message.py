
from cPickle import dumps
from hashlib import md5


class MsgType(object):
    TYPE = 'type'
    DST = 'dest'
    DATA = 'data'
    DATA_ID = 'did'
    SOURCE = 'src'
    DATA_MSG = 1
    DATA_ACK = 2 
    NEIGHBOR_ADD = 3
    NEIGHBOR_RMV = 4
    AGENT_TEST_MSG = 5
    AGENT_TETS_ACK = 6
    KEEP_ALIVE = 8
    KEEP_ALIVE_ACK = 9
    DATA_MOVE_MSG = 10
    DATA_NACK = 11
    FIFO_STATS_QUERY = 12
    PR_REQ = 13
    PR_OBJ = 14
    PR_SETUP = 15
    PR_GET_HUNGRY = 16
    PR_REQ_ACK = 17
    PR_OBJ_ACK = 18
    PR_ACK = 19
    PR_STATUS_UPDATE = 20
    PR_STATUS_ACK = 21
    

class MsgFactory(object):
    
    @staticmethod
    def create(mtype, dst=None, data=None, dataId=None, src=None):
        msg = {}
        msg[MsgType.TYPE] = mtype
        msg[MsgType.DST] = dst
        msg[MsgType.DATA] = data
        msg[MsgType.DATA_ID] = dataId
        msg[MsgType.SOURCE] = src
        msg = dumps(msg)
        return msg
        
    @staticmethod
    def generateMessageId(data):
        dig = md5()
        dig.update(str(data))
        return dig.hexdigest() 