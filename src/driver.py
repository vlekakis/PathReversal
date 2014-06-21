import logging
import argparse
import zmq
import cPickle
from sys import exit
from time import sleep
from NodeStatus import NodeStatus
from Message import MsgType
from Message import MsgFactory
from multiprocessing import Process
from FifoNode import FifoNode
from Queue import Queue, Empty
from AgentSink import AgentSinkServer
from shutil import rmtree
from os import mkdir
from random import choice
from PathReversal import PRStatusUpdate
from PathReversal import PRStatus



Nodes = {}
NodeObjectStatus = {}
LOGFILE = "objectService.log"

helpLog = {PRStatus.EATING: "Eating", 
           PRStatus.THINKING: "Thinking",
           PRStatus.HUNGRY: "Hungry"}


def isLocalNode(ip):
    if ip == 'localhost':
        return True
    elif ip == '127.0.0.1':
        return True
    return False

def establishLocalNode(nodeName, pubAgentAddr, sinkAgentAddr, neighbor):
    fifoNode = FifoNode()
    netWorker = Process(target=fifoNode.runFifoNetWorker, 
                        args=(nodeName, pubAgentAddr, sinkAgentAddr, neighbor))
    netWorker.start()
    print 'Node', nodeName , 'initiated'
    return netWorker
    

def establishRemoteNode(node):
    print 'Node', node['ip'], node['port'],  'has been initiated'

def initiateNodes(filename, pubAgentAddr, sinkAgentAddr, manual):
    
    peersHosts = []
    fp = open(filename)
    for host in fp:
        print host
        host = host.rstrip("\n")
        if ':' not in host:
            print 'Not valid node file, the program will exit'
            exit(-1)
        if host not in Nodes.keys():
            Nodes[host] = {}
        else:
            print 'Node already exists, the program will skip this node'
            continue
        node = host.split(':')
        Nodes[host]['ip'] = node[0]
        Nodes[host]['port'] = node[1]
        Nodes[host]['status'] = NodeStatus.DRIVER_PARSED
        Nodes[host]['name'] = host
        peersHosts.append(host)
    
    for h in peersHosts:
        neighbor = peersHosts[(peersHosts.index(h)+1)%len(peersHosts)]
        if isLocalNode(Nodes[h]['ip']) == True and manual == False:
            res = establishLocalNode(h, pubAgentAddr, sinkAgentAddr, neighbor)
            Nodes[host]['status'] = NodeStatus.DRIVER_INITIALIZED
            
        else:
            print 'Remote node, not supported yet'
    return peersHosts   

    
def moveData(entranceNode, dest, data=None, dataId=None, sock=None, msgtype=None):
    
    packet = MsgFactory.create(msgtype, dest, data, dataId)
    netMsg = [entranceNode, packet]
    sock.send_multipart(netMsg)
        

def queryNodeFifoStats(host, sck, msgQ): 
    moveData(host, host, sock=sck, msgtype=MsgType.FIFO_STATS_QUERY)
    while True:
        try:
            while msgQ.empty() == False:
                msg = msgQ.get(False)
                assert len(msg) == 2
                msgIn = cPickle.loads(msg[1])
                print msgIn.tx
                print msgIn.rx
                msgQ.task_done()
                return
        except Empty:
            continue
                
       
  
def verifyDataMovement(entranceNode, dest, data, sock, msgQ, dataId=None):
    
    if dataId == None:
        dataId = MsgFactory.generateMessageId(data)
        moveData(entranceNode, dest, data, dataId, sock, MsgType.DATA_MSG)
    elif dataId != None:
        moveData(entranceNode, dest, data, dataId, sock, MsgType.DATA_MOVE_MSG)
    
    while True:
        try:
            while msgQ.empty() == False:
                msg = msgQ.get(False)
                assert len(msg) == 2
                msgIn = cPickle.loads(msg[1])
                if msgIn[MsgType.TYPE] == MsgType.DATA_ACK and \
                    msg[0] == dest and msgIn[MsgType.DATA_ID] == dataId:
                    msgQ.task_done()
                    print "Message with id", dataId, " arrived to dest", dest
                    return
                if msgIn[MsgType.TYPE] == MsgType.DATA_NACK \
                     and msgIn[MsgType.DATA_ID] == dataId:
                    msgQ.task_done()
                    print "Message with id", dataId, " was not in the dest", msg[0]
                    return 
        except Empty:
            continue
                
                    
                

def testBidirectionalChannel(sock, msgInQ, msgOutQ):
    
    mCheck=0
    packet = MsgFactory.create(MsgType.AGENT_TEST_MSG)
    for k in Nodes.keys():
        msg = [Nodes[k]['name'], packet]
        sock.send_multipart(msg)
        
    while True:
        try:
            while msgInQ.empty() == False:
                msg = msgInQ.get(False)
                Nodes[msg[0]]['status'] = NodeStatus.DRIVER_FUNCTIONAL
                msgInQ.task_done()
                msgOutQ.put([msg[0], "ack"])
                
                mCheck+=1
                
                if mCheck==len(Nodes.keys()):
                    print 'Received all ACK messages'
                    return
                    
        except Empty:
            if mCheck == 3:
                break
            else:
                print 'Exception'
                continue
             
 
def buildScenario(scenarioFile):
    try:
        scenario = []
        fp = open(scenarioFile)
        for line in fp:
            if line.startswith('#'):
                continue
            line = line.rstrip('\n')
            line = line.split()
            action = {}
            for argument in line:
                #print argument
                if argument == 'sleep' or argument == 'set' or \
                    argument == 'reset' or argument == 'hungry' or \
                    argument == 'exit':
                    action['ACTION'] = argument
                else:
                    action['ARG'] = argument
            scenario.append(action)
        
        return scenario            
    except IOError as e:
        print e.message()
        print 'The program will exit'
        exit(1) 
 
def verifyObjectTransferService(hungryQ):
    try:
        nodeExpectedToBeEating = hungryQ.get(False)
        print "Node expected to be Eating",  nodeExpectedToBeEating
        for n in NodeObjectStatus.keys():
            if n != nodeExpectedToBeEating:
                assert NodeObjectStatus[n] != PRStatus.EATING
            else:
                assert NodeObjectStatus[n] == PRStatus.EATING
    except Empty:
        print "There should be at least on hungry node by now"
        assert False

    logging.info("-----ObjectService-Verification------")
    for n in NodeObjectStatus.keys():
        logging.info("Node "+n+" status: "+helpLog[NodeObjectStatus[n]])
        

def verifyOperation(update, hungryQ):
    node = update[0]
    update = cPickle.loads(update[1])
    
    if node not in NodeObjectStatus.keys():
        NodeObjectStatus[node] = "N/A"
    
    print "Current Node Status(", node, ")", NodeObjectStatus[node]
    NodeObjectStatus[node] = update[PRStatusUpdate.STATUS]
    print "Changed Node Status(", node, ")", NodeObjectStatus[node]
    if update[PRStatusUpdate.SATISFY] == True:
        print 'Satisfy-Message'
        verifyObjectTransferService(hungryQ)
    else:
        print 'Status-Update Message'
    
    #TODO: LR-part
    
    statusAck = PRStatusUpdate.createStatusACKMessage(update[PRStatusUpdate.SEQ])
    return (node, statusAck)
 
def playScenario(scenarioFile, peerSock, inQueue, outQueue, sinkServer):           
    
    hungryQ = Queue()
    scenario = buildScenario(scenarioFile)
    logging.basicConfig(level=logging.INFO, filename=LOGFILE)
    
    for cmd in scenario:
    
        try:
            print 'Testing if there is anything available in the Queue'
            updateAck = inQueue.get(True, timeout=1)
            inQueue.task_done()
            node, statusAck = verifyOperation(updateAck, hungryQ)
            
            print 'Sending ACK back to the ', node            
            outQueue.put([node, statusAck])
                
            
            
        except Empty:
            print 'Queue Empty'
            pass
        
        if cmd['ACTION'] == 'sleep':
            print 'Waiting for ', cmd['ARG']
            sleep(int(cmd['ARG']))
            
        elif cmd['ACTION'] == 'set':
            dataItem = str(choice(xrange(1000)))
            dataId = MsgFactory.generateMessageId(dataItem)
            txMsg = MsgFactory.create(MsgType.PR_SETUP, 
                                      dst=cmd['ARG'], 
                                      data=dataItem,
                                       dataId=dataId)
            print 'Setting object', dataItem, 'to node:', cmd['ARG']
            peerSock.send_multipart(['Set', txMsg])
            
        elif cmd['ACTION'] == 'reset':
            print 'Reseting nodes'
            peerSock.send_string('Reset')
            
        elif cmd['ACTION'] == 'hungry':
            hungryQ.put_nowait(cmd['ARG'])
            print 'Set Hungry node ', cmd['ARG']
            txMsg = MsgFactory.create(MsgType.PR_GET_HUNGRY, dst=cmd['ARG'])
            peerSock.send_multipart([cmd['ARG'], txMsg])
        
        elif cmd['ACTION'] == 'exit':
            print 'Exiting'
            peerSock.send_string('Exit')
            sinkServer.join()
            inQueue.join()
            exit(0)
        
def main():
    
    p = argparse.ArgumentParser(description='Driver for 712-Project')

    
    p.add_argument('-n', dest='nodes', action='store', default=None,
                   help='Nodes file with the format <address:port> or <localhost:port>')
    

    p.add_argument('-l', dest='logDirectory', action='store', default='logs',
                   help='Location of the peer log files')
    
    p.add_argument('-c', dest='cleanLogs', action='store_true', default=False,
                   help='Decide to empty the log files')
    
    
    p.add_argument('-s', dest='scenarioFile', action='store', default=None,
                   help = 'Scenario file that holds the Path Reversal scenario')
    
    
    p.add_argument('--manual', dest='manual', action='store_true', default=False,
                   help='Manual fifonodes')
    
    
    
    args = p.parse_args()
    
    if args.nodes == None:
        print 'Nodes file is not given...Program will exit'
        exit(1)
    
    if args.scenarioFile == None:
        print 'Please provide a scenario file...Program will exit'
        exit(1)
    
    if args.cleanLogs == True:
        rmtree(args.logDirectory, ignore_errors=True)
        mkdir('logs')
    
    
    sinkSockBindAddr = "tcp://*:9090"
    pubSockBindAddr = "tcp://*:5558" 
    sinkAddr = "tcp://127.0.0.1:9090"
    pubAgentBindAddress = 'tcp://127.0.0.1:5558'
    
    context = zmq.Context()
    print 'Creating PeerSink server...'
    peerIncomingQueue = Queue(10)
    peerOutgoingQueue = Queue(10)
    sinkSock = context.socket(zmq.REP)
    
    sinkServer = AgentSinkServer(peerIncomingQueue, peerOutgoingQueue,
                                 sinkSock, sinkSockBindAddr)
    sinkServer.start()
    

    
    print 'Initiating nodes from file....', args.nodes
    peerHosts = initiateNodes(args.nodes, pubAgentBindAddress, sinkAddr, args.manual)
    
    
    print 'Creating pub-Agent socket'
    ctrlSock = context.socket(zmq.PUB)
    ctrlSock.bind(pubSockBindAddr)
    
    print 'Waiting for  setup to finish'
    sleep(5)
    print '# # # # # # # # # # #  # # # # # # #  # # # # #'
    
    print 'Test Nodes Communication channel with the Agent'
    testBidirectionalChannel(ctrlSock, peerIncomingQueue, peerOutgoingQueue)
    print '@ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @'    
    
    print 'Establish connections with the Neighbors'
    ctrlSock.send_string('ConnectToNeighbor')
    sleep(1)
    print '# # # # # # # # # # #  # # # # # # #  # # # # #'
    
    if len(peerHosts) > 1:
        print 'Test peer connection with their Neighbors'
        ctrlSock.send_string('TestConnectionToNeighbor')
        print '@ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @'
  
    sleep(5)
    playScenario(args.scenarioFile, ctrlSock, 
                 peerIncomingQueue, peerOutgoingQueue, sinkServer)

    
if __name__ == "__main__":
    main()