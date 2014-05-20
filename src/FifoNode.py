import zmq
from sys import exit
import logging



def fifoNetWorker(netId, ctrlIp, ctrlPort, serverPort, name):
    LOG_FILENAME = 'example.log'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
    context = zmq.Context()
    
    netCtrlRxSock = context.socket(zmq.PULL)
    netCtrlRxSock.connect('tcp://'+ctrlIp+':'+ctrlPort)
    poller = zmq.Poller()
    poller.register(netCtrlRxSock, zmq.POLLIN)
    
    
    while True:
        logging.debug('only polling')
        socks = dict(poller.poll())
        if socks.get(netCtrlRxSock) == zmq.POLLIN:
            controlMsg = netCtrlRxSock.recv()
            if controlMsg == 'EXIT':
                logging.debug('Time to quit...')
                exit(1)
            logging.debug("Recvied: "+str(controlMsg))
            