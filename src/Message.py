from json import dumps
from hashlib import md5

class MsgType(object):
    TYPE = 'type'
    DST = 'dest'
    DATA = 'data'
    DATA_ID = 'did'
    DATA_MSG = 1
    DATA_ACK = 2 
    NEIGHBOR_ADD = 3
    NEIGHBOR_RMV = 4
    AGENT_TEST_MSG = 5
    AGENT_TETS_ACK = 6
    SOURCE =  7
    KEEP_ALIVE=8
    KEEP_ALIVE_ACK=9

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