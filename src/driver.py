
import argparse
from sys import exit
from time import sleep
from NodeStatus import NodeStatus
import zmq
from multiprocessing import Process
from FifoNode import fifoNetWorker

Nodes = {}

def ctrlVentilator(ctrlPort, cmdPort):
   
    exit(1)
    

def isLocalNode(ip):
    if ip == 'localhost':
        return True
    elif ip == '127.0.0.1':
        return True
    return False

def establishLocalNode(node):
    netWorker = Process(target=fifoNetWorker, args=(1,'localhost','5558', '', ''))
    netWorker.start()
    print 'Node', node['ip'], node['port'],  'has been initiated'
    

def establishRemoteNode(node):
    print 'Node', node['ip'], node['port'],  'has been initiated'

def initiateNodes(filename):
    
    fp = open(filename)
    for host in fp:
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
  
    for n in Nodes.keys():
        if Nodes[n]['status'] == NodeStatus.DRIVER_PARSED:
            if isLocalNode(Nodes[n]['ip']) == True:
                res = establishLocalNode(Nodes[n])
            else:
                res = establishRemoteNode(Nodes[n])
            
            if res == 0:
                Nodes[n]['status'] = NodeStatus.DRIVER_INITIALIZED
  
  
        
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
    initiateNodes(args.nodes)
    context = zmq.Context()
    ctrlSock = context.socket(zmq.PUSH)
    ctrlSock.bind("tcp://127.0.0.1:5558")
    print 'Test'
    sleep(1)
    for msg in xrange(100):
        ctrlSock.send(str(msg))
        
    
    ctrlSock.send('EXIT')
    
if __name__ == "__main__":
    main()