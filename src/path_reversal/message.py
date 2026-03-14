"""Message types and factory for inter-node communication."""

from pickle import dumps
from hashlib import md5
from typing import Any, Optional


class MsgType:
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
    AGENT_TEST_ACK = 6
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
    # Legacy typo alias
    AGENT_TETS_ACK = AGENT_TEST_ACK


class MsgFactory:

    @staticmethod
    def create(mtype: int, dst: Optional[str] = None, data: Any = None,
               dataId: Optional[str] = None, src: Optional[str] = None) -> bytes:
        msg = {
            MsgType.TYPE: mtype,
            MsgType.DST: dst,
            MsgType.DATA: data,
            MsgType.DATA_ID: dataId,
            MsgType.SOURCE: src,
        }
        return dumps(msg)

    @staticmethod
    def generateMessageId(data: Any) -> str:
        dig = md5()
        dig.update(str(data).encode())
        return dig.hexdigest()
