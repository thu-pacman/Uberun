#!/usr/bin/python3
import time
import os
import sys
from SSnetwork import SSMasterNetwork
from SSdatabase import SSDatabase 
from SSscheduler import SSScheduler
from SSprotocol import SSProtocol
from SSlogger import SSLogger
from SSparser import SSParser

class SSMaster:
    def __init__(self, algoname='CE', alpha=0.9):
        self.MIN_DAEMONS = 8
        self.net = SSMasterNetwork()
        self.db = SSDatabase(algorithm=algoname)
        self.sched = SSScheduler(algoname=algoname, database=self.db)
        self.default_alpha = alpha
        self.prtl = SSProtocol()
        self.logger = SSLogger('Master')
        self.parser = SSParser()
        self.users = []
        self.daemons = []
    
    def isclean(self):
        if len(self.db.pendingJobs) or len(self.db.runningJobs):
            return False
        else:
            return True

    def addJobSequence(self, jobstring):
        for n in jobstring.split(','):
            n = n.strip()
            fm = None
            exe = n.split('-')[0]
            if exe in ['gan', 'rnn']:
                fm = 'TensorFlow'
            elif exe in ['ts', 'nw', 'wc']:
                fm = 'Spark'
            else:
                fm = 'MPI'
            self.db.addUserJob({'jobname': n, 'framework': fm, 'parallelism': int(n[-2:]), 'alpha': self.default_alpha})

    def parse(self):
        self.parser.addRecords(self.parser.loadHistory(self.db.history))
        return self.parser.getBasicStats(self.parser.selectRecords())
    
    # the main loop
    def run(self):
        # try to get new message
        client, msg = self.net.recvObj(timeout=1)
        if client: # acts accordingly
            # connection broken
            if msg == self.net.CONNECTION_BROKEN: # client lost
                if client in self.users:
                    self.users.remove(client)
                if client in self.daemons:
                    self.logger.error('No handle for daemon lost !!')
                    self.daemons.remove(client)
                    #TODO database remove, scheduler reschedule
            # normal messages
            #self.logger.echo(msg)
            if self.prtl.isgreeting(msg): # new client
                if msg['role'] == 'user':
                    self.logger.debug('New User from', client)
                    self.users.append(client) # user for interaction
                elif msg['role'] == 'daemon':
                    self.logger.debug('New Daemon from', client)
                    self.daemons.append(client) # daemon run on each job
                    self.db.addDaemon(client, msg['hostname'])
            elif self.prtl.isjobfinish(msg):
                # NOTE, only one daemon of the job finish, need all finish to really finish
                self.db.daemonFinishJob(client, msg['jobid'], msg['returns']) 
        # wait for all daemons 
        if len(self.daemons) < self.MIN_DAEMONS:
            return
        # try to schedule jobs, ignore the estimate time
        allocation, _ = self.sched.nextJob()
        #self.logger.debug(allocation)
        if allocation:
            for daemon, jobspec in allocation:
                #self.logger.echo(daemon, jobspec)
                self.net.sendObjTo(daemon, self.prtl.newjob(jobspec))
        
if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('Usage: ./SSmaster.py Algo(CE/CS/SS) JOB_SEQUENCE ALPHA')
    sched_algo = sys.argv[1].strip()
    job_sequence = sys.argv[2].strip()
    alpha = float(sys.argv[3])
    print('Going to use %s algorithm (alpha=%.2f) for jobs: %s' % (sched_algo, alpha, job_sequence))

    master = SSMaster(algoname=sched_algo, alpha=alpha)
    master.addJobSequence(job_sequence)
    master.logger.succ('Master started, will schedule jobs after daemons connected.')
    while not master.isclean():
        master.run() 
    bs = master.parse()
    jobcnt = len(master.parser.records)
    header = '%30s\t%8s\t%8s\t%8s\t%8s\t%8s\t%8s\t%8s\t%s' % ('Algo', 'ALPHA', 'OCC(%)', 'MAX_TURN', 'USE_CH', 'BUB_CH', 'JOB_WAIT', 'JOB_RUN', 'HISTORY_FILE')
    result = '%30s\t%8.2f\t%8.2f\t%8.2f\t%8.0f\t%8.0f\t%8.0f\t%8.0f\t%s' % (
        master.sched.algo.name, 
        master.default_alpha,
        bs['occupation'],
        bs['max_turnaround'], 
        bs['use_corehours'],
        bs['bubble_corehours'],
        sum(bs['jobwaittimes'])/jobcnt,
        sum(bs['jobruntimes'])/jobcnt,
        master.db.historyFilename)
    print(header)
    print(result)

    with open('results.txt', 'a+') as fw:
        fw.write('Algorithm %s JobSequence %s\n' % (sched_algo, job_sequence))
        #fw.write('%s\n' % header)
        fw.write('%s\n' % result)

    