
import argparse
import zmq
from sys import exit
from time import sleep
from NodeStatus import NodeStatus
from Message import MsgType
from Message import MsgFactory
from multiprocessing import Process
from FifoNode import FifoNode
from Queue import Queue, Empty
from AgentSink import AgentSinkServer


Nodes = {}


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

def initiateNodes(filename, pubAgentAddr, sinkAgentAddr):
    
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
        if isLocalNode(Nodes[h]['ip']) == True:
            res = establishLocalNode(h, pubAgentAddr, sinkAgentAddr, neighbor)
            Nodes[host]['status'] = NodeStatus.DRIVER_INITIALIZED
            
        else:
            print 'Remote node, not supported yet'
    return peersHosts   

    
def moveData(entranceNode, dest, data, sock):
    packet = MsgFactory.create(MsgType.DATA_MSG, dest, data, None)
    netMsg = [entranceNode, packet]
    sock.send_multipart(netMsg)
               
        
  
def testBidirectionalChannel(sock, msgQ):
    
    mCheck=0
    packet = MsgFactory.create(MsgType.AGENT_TEST_MSG)
    for k in Nodes.keys():
        msg = (Nodes[k]['name'], packet)
        sock.send_multipart(msg)
        
    while True:
        try:
            while msgQ.empty() == False:
                msg = msgQ.get(False)
                Nodes[msg[0]]['status'] = NodeStatus.DRIVER_FUNCTIONAL

                mCheck+=1
                msgQ.task_done()
                if mCheck==len(Nodes.keys()):
                    print 'Received all ACK messages'
                    return
                    
        except Empty:
            if mCheck == 3:
                break
            else:
                print 'Exception'
                continue
             
            
  
        
def main():
    
    p = argparse.ArgumentParser(description='Driver for 712-Project')

    
    p.add_argument('-n', dest='nodes', action='store', default=None,
                   help='Nodes file with the format <address:port> or <localhost:port>')
    
    p.add_argument('-k', dest='keyboard', action='store_true', default=False,
                   help='Keyboard driven run of the system')
    
    args = p.parse_args()
    
    if args.nodes == None:
        print 'Nodes file is not given...Program will exit'
        exit(1)
    
    sinkSockBindAddr = "tcp://*:9090"
    pubSockBindAddr = "tcp://*:5558" 
    sinkAddr = "tcp://127.0.0.1:9090"
    pubAgentBindAddress = 'tcp://127.0.0.1:5558'
    
    
    context = zmq.Context()
    print 'Creating PeerSink server...'
    peerQueue = Queue(10)
    sinkSock = context.socket(zmq.PULL)
    
    sinkServer = AgentSinkServer(peerQueue, sinkSock, sinkSockBindAddr)
    sinkServer.start()
    
    print 'Initiating nodes from file....', args.nodes
    peerHosts = initiateNodes(args.nodes, pubAgentBindAddress, sinkAddr)
    
    
    print 'Creating pub-Agent socket'
    ctrlSock = context.socket(zmq.PUB)
    ctrlSock.bind(pubSockBindAddr)
    print 'Waiting for  setup to finish'
    print '# # # # # # # # # # #  # # # # # # #  # # # # #'
    sleep(1)
    
    print 'Test Nodes Communication channel with the Agent'
    testBidirectionalChannel(ctrlSock, peerQueue)
    print '@ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @'    
    
    
    print 'Establish connections with the Neighbors'
    ctrlSock.send_string('ConnectToNeighbor')
    print '# # # # # # # # # # #  # # # # # # #  # # # # #'
    
    sleep(1)
    
    print 'Test peer connection with their Neighbors'
    ctrlSock.send_string('TestConnectionToNeighbor')
    print '@ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @ @'
    
    
    
    moveData(peerHosts[0], peerHosts[0], "DataTest")
    
    
    sleep(2)
    print 'Exiting'
    ctrlSock.send_string('Exit')
    print '# # # # # # # # # # #  # # # # # # #  # # # # #'
    sinkServer.join()
    peerQueue.join()
    exit(1)
    
    
if __name__ == "__main__":
    main()