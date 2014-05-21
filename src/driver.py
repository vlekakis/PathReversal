
import argparse
from sys import exit
from time import sleep
from NodeStatus import NodeStatus
import zmq
from multiprocessing import Process
from FifoNode import FifoNode
import json

Nodes = {}

def ctrlVentilator(ctrlPort, cmdPort):
   
    exit(1)
    

def isLocalNode(ip):
    if ip == 'localhost':
        return True
    elif ip == '127.0.0.1':
        return True
    return False

def establishLocalNode(nodeName, ctrlAddress):
    fifoNode = FifoNode()
    netWorker = Process(target=fifoNode.runFifoNetWorker, args=(nodeName, ctrlAddress))
    netWorker.start()
    print 'Node', nodeName , 'initated'
    return netWorker
    

def establishRemoteNode(node):
    print 'Node', node['ip'], node['port'],  'has been initiated'

def initiateNodes(filename, ctrlAddress):
    
    workers = []
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
        Nodes[host]['name'] = "tcp://"+host
  
        if isLocalNode(Nodes[host]['ip']) == True:
            res = establishLocalNode(host, ctrlAddress)
            Nodes[host]['status'] = NodeStatus.DRIVER_INITIALIZED
            workers.append(res)
        else:
            print 'Remote node, not supported yet'
            
    return workers
            
        
  
def testServerNodes(context):
    sock = context.socket(zmq.REQ)
    for k in Nodes.keys():
        sock.connect(Nodes[k]['name'])
        sock.send_string("Test")
        print "AFTER SEND"
        msg = sock.recv_string()
        print 'Node' , k, 'replied' , msg
        
  
        
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
    
    print 'Initiating nodes from file....', args.nodes
    ctrlAddress = 'tcp://127.0.0.1:5558'
    w = initiateNodes(args.nodes, ctrlAddress)
    
    
    context = zmq.Context()
    ctrlSock = context.socket(zmq.PUB)
    ctrlSock.bind(ctrlAddress)
    print 'Test'
    sleep(1)
     
#     serverPort = json.dumps({'server':'5000'})
#     ctrlSock.send_multipart(['localhost:5000', serverPort ])
    #testServerNodes(context)    
    ctrlSock.send_string('Exit')
    
    exit(1)
    
    
if __name__ == "__main__":
    main()