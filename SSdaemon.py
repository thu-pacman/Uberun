#!/usr/bin/python3
import time
from SSnetwork import SSWorkerNetwork
from SSprotocol import SSProtocol
from SSlogger import SSLogger
from SSjobrunner import SSJobRunner

class SSDaemon:
    def __init__(self):
        self.net = SSWorkerNetwork()
        self.prtl = SSProtocol()
        self.logger = SSLogger('Daemon')
        #self.msgLock = threading.Lock() 
        self.jobrunners = []
        #self.profiler = None
        time.sleep(1) # if not wait, will fail to connect, reason unknown
        self.net.sendObj(self.prtl.greeting('daemon', self.net.hostname)) # I am a daemon
        #self.net.sendObj(self.prtl.machineinfo({'hostname': self.net.hostname, 'core': 28, 'llcway': 20, 'membw': 120}))
    
    def run(self):
        # try to get new message
        master, msg = self.net.recvObj()
        if master:
            if msg == self.net.CONNECTION_BROKEN:
                exit()
            # acts accordingly
            if self.prtl.isnewjob(msg):
                runner = SSJobRunner(self.net.hostname, msg['jobspec'], name='Jobrunner@'+self.net.hostname)
                runner.start()
                self.jobrunners.append(runner)
        # try to check job completion
        done_runners = []
        for jr in self.jobrunners:
            if not jr.is_alive(): # job finish
                self.net.sendObj(self.prtl.jobfinish(jr.jobspec['jobid'], jr.returns)) 
                done_runners.append(jr)
        for jr in done_runners:
            self.jobrunners.remove(jr)
    
if __name__ == '__main__':
    daemon = SSDaemon()
    while True:
        daemon.run() 

