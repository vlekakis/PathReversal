import zmq
from sys import exit
from sys import argv
import logging
import json



def fifoNetWorker(netName, ctrlAddress):
    
    logname = netName.replace(':', '_')+'.log'
    LOG_FILENAME = logname
    logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
    
    context = zmq.Context()
    
    netCtrlRxSock = context.socket(zmq.SUB)
    netCtrlRxSock.setsockopt(zmq.SUBSCRIBE, netName)
    netCtrlRxSock.setsockopt(zmq.SUBSCRIBE, b'Exit')
    netCtrlRxSock.connect(ctrlAddress)
    
    
    poller = zmq.Poller()
    poller.register(netCtrlRxSock, zmq.POLLIN)
    
    
    while True:
        socks = dict(poller.poll())
        if socks.get(netCtrlRxSock) == zmq.POLLIN:
            msg = netCtrlRxSock.recv_string()
            logging.debug("LocalName:\t"+netName+'\t message\t'+msg )
            if msg == 'Exit':
                exit(1)

