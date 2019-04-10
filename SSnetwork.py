import socket
import selectors
import types
import json
from SSlogger import SSLogger
from SSconfig import SSConfig as CFG

'''
SSNetwork provides python-objects send-recv interface to SS modules over network.
'''
class SSNetwork:
    def __init__(self, mode='worker'):
        self.logger = SSLogger('Network', info=False, echo=False)
        self.hostname = socket.gethostname()
        # mode: master or worker
        self.mode = mode
        # selector use for non-blocking io 
        self.sel = selectors.DefaultSelector()
        # connections, clinet -> connection
        self.connections = dict()
        # obj buffer for each connection, connection -> object list
        # buf[0] is the tailing string (without a '#' end flag)
        # buf[1] and after are completed commands
        self.objectBuffer = dict()
        # constant values
        self.EOC = CFG.NET['eoc']
        self.CONNECTION_BROKEN = CFG.NET['broken_conn_str']
        self.NEW_CONNECTION = CFG.NET['new_conn_str']
        self.SS_MASTER = socket.gethostbyname(CFG.NET['master_hostname']) 
        self.SS_PORT = CFG.NET['master_port']
        self.BACK_LOG = CFG.NET['master_backlog']

        # master and worker
        # master connects to all workers,
        # worker only connects to master, no inter-worker connections
        if mode == 'master': # master
            lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPV4 and TCP
            lsock.bind(('', self.SS_PORT)) # accept from any
            lsock.listen(self.BACK_LOG)
            # use selector for non blocking IO, only check READ, assume always writable
            lsock.setblocking(False)
            self.sel.register(lsock, selectors.EVENT_READ, data=self.NEW_CONNECTION) 
            self.logger.info('Master started on %s' % socket.gethostname())
        else: # worker, both daemons and user frontends
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            sock.connect_ex((self.SS_MASTER, self.SS_PORT))
            #data = types.SimpleNamespace(workerName=socket.gethostname())
            #workerName = socket.gethostname()
            self.sel.register(sock, selectors.EVENT_READ, data='master')
            self.connections['master'] = sock # only connects to master
            self.objectBuffer[sock] = ['']
            self.logger.info('Daemon started on %s' % socket.gethostname())
    
    # send object to destination
    # 1. use json to serialize an object to string, only basic python types supported
    # 2. append EOC to the string, so that object strings can be separated on remote
    # 3. sendall, we dont send a string in multiple times. sendall is blocking but should work in our case
    def sendObjTo(self, destination, obj=None):
        #print('To Send >>', obj)
        wrapMsg = json.dumps(obj) + self.EOC
        self.connections[destination].sendall(wrapMsg.encode('utf-8'))

    # recv an object from anywhere
    # return value: (source, object received)
    # 1. pick an object from any buffer and return both the source and the object
    # 2. check new connection, if any, connect
    # 3. check new data fron network, if any, buffer it
    def recvObj(self, timeout=1):
        # find someone has objects, and return the first pending object
        for client, conn in self.connections.items():
            buf = self.objectBuffer[conn]
            if len(buf) >= 2:
                obj = buf.pop(1)
                if obj == self.CONNECTION_BROKEN: # all the objects from a broken have been received
                    self.logger.info(client, 'lost connection')
                    assert(buf[0] == '') # there should be not tailing incomplete string
                    self.objectBuffer.pop(conn) # remove the entry for broken connection
                    self.connections.pop(client) # also remove the connection
                return (client, obj)
        sourcelist = []
        # check if something to read from socket
        events = self.sel.select(timeout=timeout)
        for key, mask in events:
            assert(mask & selectors.EVENT_READ)
            # new connection, only master should receive this
            if key.data is self.NEW_CONNECTION:
                assert(self.mode == 'master')
                sock = key.fileobj
                conn, addr = sock.accept() 
                conn.setblocking(False)
                #worker = socket.gethostbyaddr(addr[0])[0] # hostname of new connecting machine
                #print('accepted connection from', addr)
                self.sel.register(conn, selectors.EVENT_READ, data=addr) # use addr to distinguish clients
                # TODO, re-connect workers
                if addr in self.connections:
                    print('Currently we dont handle this case')
                    assert(False)
                self.connections[addr] = conn # record new connection
                self.objectBuffer[conn] = ['']
                # for new connection event, we don't need to receive data
            else:
                sourcelist.append(key.data) # append a source that has data here, key.data is 'addr' of client
        # now read data from all connections that have data
        for source in sourcelist:
            conn = self.connections[source]
            # entry should be added when connection built
            assert(conn in self.objectBuffer)
            buf = self.objectBuffer[conn]
            # receive whatever it can
            s = conn.recv(1024).decode('utf-8') # a string that may have <1, =1, >1 dumped objects
            if len(s) == 0: # connection broken
                # NOTE !!! DO NOT pop connection from the buffer immediatly since it may have unread objects
                # DON'T DO THIS: self.commandBuffer.pop(conn)
                # However, the connection can be unregister
                self.sel.unregister(conn)
                # append the broken info to the object buffer
                buf.append(self.CONNECTION_BROKEN)
            else: # normal string
                ss = s.split(self.EOC) # split by EOC, each slice is an object or a pice of an object
                if len(ss) == 1: # incomplement
                    buf[0] = buf[0] + ss[0] # still incomplete
                else:
                    buf.append(json.loads(buf[0]+ss[0])) # the incomplete buf[0] now completed, de-serialized
                    for i in range(1, len(ss)-1): # middle slices must tbe completed
                        buf.append(json.loads(ss[i]))
                    buf[0] = ss[-1] # the tail may be incomplete
        return (None, None) # nothing

class SSMasterNetwork(SSNetwork):
    def __init__(self):
        super().__init__(mode='master')

class SSWorkerNetwork(SSNetwork):
    def __init__(self):
        super().__init__(mode='worker')
    def sendObj(self, obj=None):
        super().sendObjTo('master', obj)
