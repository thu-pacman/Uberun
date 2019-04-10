#!/usr/bin/python3
import time
import numpy as np
import heapq
from datetime import datetime
from SSdatabase import SSDatabase 
from SSscheduler import SSScheduler
from SSlogger import SSLogger
from SSparser import SSParser
from SSconfig import SSConfig as CFG
import SSjobgenerator

class SimulationClock:
    def __init__(self):
        self.ts = 0
    def tick(self):
        self.ts += 1
    def ticksto(self, tt):
        self.ts = tt
    def now(self):
        return self.ts

class SSSimulator:
    def __init__(self, alg='CE'):
        self.MIN_DAEMONS = 1
        self.clock = SimulationClock()
        self.db = SSDatabase(algorithm=alg, simulationClock=self.clock, logToFile=False)
        self.sched = SSScheduler(algoname=alg, database=self.db)
        self.logger = SSLogger('Simulator')
        self.parser = SSParser()
        self.users = []
        self.daemons = []
        self.trace = []
        self.pendingJobs = dict()
        self.runningJobs = dict()
    
    def isclean(self):
        if len(self.db.pendingJobs) or len(self.db.runningJobs) or len(self.trace):
            return False
        else:
            return True
    
    def addTrace(self, trace):
        self.trace.extend(trace)
        self.trace.sort(key=lambda x: x[2])

    def loadTrace(self, fname):
        with open(fname, 'r') as fr:
            for line in fr.readlines():
                program, nproc, submittime, duration = line.strip().split(',')
                self.trace.append((program, int(nproc), float(submittime), float(duration)))
        self.trace.sort(key=lambda x: x[2])
        jobs = ', '.join([x[0] for x in self.trace])
        self.logger.info('Job trace: ', jobs)
        #self.logger.echo(self.trace)
        
    def addFakeDeamons(self, prefix, cnt):
        for i in range(0, cnt):
            fakeDeamon = prefix + str(i)
            self.daemons.append(fakeDeamon)
            self.db.addDaemon(fakeDeamon, fakeDeamon)
        #self.logger.info('Daemons:', self.daemons)
    
    # the main loop
    def run(self, alpha=0.9):
        done_cnt = 0
        next_time = [x[2] for x in self.trace]
        heapq.heapify(next_time)
        while not self.isclean():
            # new coming job at now
            # trace is already sorted by submit time
            while len(self.trace) and self.trace[0][2] <= self.clock.now():
                fm = None
                n = self.trace[0][0]
                exe = n.split('-')[0]
                if exe in ['gan', 'rnn']:
                    fm = 'TensorFlow'
                elif exe in ['ts', 'nw', 'wc']:
                    fm = 'Spark'
                else:
                    fm = 'MPI'
                jobid = self.db.addUserJob({'jobname': n, 'framework': fm, 'parallelism': self.trace[0][1], 'alpha': alpha})
                self.pendingJobs[jobid] = self.trace[0]
                self.trace.pop(0)
                #print('put jid %d as pending' % jobid)

            # if be able to start new job at now
            # try to schedule jobs, and get its estimated runtime

            while True:
                allocation, est = self.sched.nextJob()
                self.logger.debug(allocation)
                if allocation:
                    if est is None:
                        print(allocation)
                    assert(est)
                    daemons = []
                    for daemon, jobspec in allocation:
                        jobid = jobspec['jobid']
                        daemons.append(daemon)
                    # compute the duration and finish time
                    jt = self.pendingJobs[jobid]
                    # if no estimation time, use the standard duration
                    est_time = est[0] if jt[3] == 0 else jt[3]*est[1]
                    et = self.clock.now() + est_time
                    # runningjob[id] = (finish time, daemons)
                    self.runningJobs[jobid] = (et, list(daemons))
                    heapq.heappush(next_time, et+1)
                else:
                    break

            # if a job finish at now
            flag = True
            while flag:
                flag = False
                for jobid, v in self.runningJobs.items():
                    # job finish
                    if v[0] <= self.clock.now():
                        for daemon in v[1]:
                            self.db.daemonFinishJob(daemon, jobid, {'exitcode': 0})
                        self.runningJobs.pop(jobid)
                        done_cnt += 1
                        if done_cnt % 500 == 0:
                            print('Simulation done for %d jobs' % done_cnt)
                            pass
                        flag = True
                        break

            if len(next_time):
                self.clock.ticksto(heapq.heappop(next_time))
            else:
                self.clock.tick()

    def parse(self):
        self.parser.addRecords(self.parser.loadHistory(self.db.history))
        return self.parser.getBasicStats(self.parser.selectRecords())
    
    def show(self):
        self.parser.showSchedFig(self.parser.selectRecords())

# For small generated traces
'''
if __name__ == '__main__':
    jss = []
    #with open('sequences.txt', 'r') as fr:
    #    for line in fr:
    #        jss.append(line.strip())
    #heavy_jobs = ['bw-16', 'bw-28', 'mg-16', 'lu-16', 'cg-16'] # ts
    #light_jobs = ['hc-16', 'hc-28', 'ep-16', 'gan-16', 'rnn-16', 'bfs-16'] # wc, nw
    progs = []
    progs.extend(['bw-28']*8)
    progs.extend(['hc-28']*5)
    #progs.extend(heavy_jobs)
    #progs.extend(light_jobs)
    job_cnt = 10000
    jss.append(','.join(np.random.choice(progs, job_cnt)))
    node_cnt = 1000
    sub_itvl = 0
    for node_cnt in [1000]:#[10, 50, 100, 200, 500, 1000, 2000, 5000]:
    #for sub_itvl in range(0, 110, 10):
        for js in jss:
            jid = 0
            #print(js)
            simCE = SSSimulator(alg='CE')
            simSS = SSSimulator(alg='SS')
            header = '%30s\t%8s\t%8s\t%8s\t%8s\t%8s\t%8s\t%8s\t%8s' % ('Algo', 'OCC(%)', 'MAX_TURN', 'USE_CH', 'BUB_CH', 'JOB_WAIT', 'JOB_RUN', 'JOB_TURN', 'CNT/ITVL')
            print(header)
            waits, runs = [], []
            for sim in [simCE, simSS]:
                trace = []
                ss = js.split(',')
                for s in ss:
                    program, nproc, submittime, duration = s, int(s[-2:]), jid*sub_itvl, 0
                    jid += 1
                    trace.append((program, int(nproc), float(submittime), float(duration)))
                sim.addTrace(trace)
                sim.addFakeDeamons('sn', node_cnt)
                sim.run(alpha=0.9)
                bs = sim.parse()
                jobcnt = len(sim.parser.records)
                result = '%30s\t%8.2f\t%8.2f\t%8.0f\t%8.0f\t%8.0f\t%8.0f\t%8.0f\t%8.0f' % (
                    sim.sched.algo.name, 
                    bs['occupation'],
                    bs['max_turnaround'], 
                    bs['use_corehours'],
                    bs['bubble_corehours'],
                    sum(bs['jobwaittimes'])/jobcnt,
                    sum(bs['jobruntimes'])/jobcnt,
                    (sum(bs['jobwaittimes'])+sum(bs['jobruntimes']))/jobcnt,
                    node_cnt,)
                #print(header)
                print(result)
                #sim.show()
    
'''
# for big traces
if __name__ == '__main__':
    # convert_mustang_trace(light_rato=0.5)
    #SSjobgenerator.convert_trinity_trace(light_rato=0.25)
    #exit()
    simCE = SSSimulator(alg='CE')
    #simCS = SSSimulator(alg='CS')
    simSS = SSSimulator(alg='SS')
    trace = []
    #with open('mustang_trace.txt', 'r') as fr:
    node_cnt = 9408
    with open('trinity_trace.txt', 'r') as fr:
        cnt = 0
        for line in fr:
            program, nproc, submittime, duration = line.strip().split(',')
            #trace.append((program, min(int(nproc), node_cnt*28), float(submittime), float(duration)))
            #trace.append((program, min(int(nproc), node_cnt//8*28), float(submittime), float(duration)))
            trace.append((program, int(nproc), float(submittime)//4, float(duration)))
            #trace.append((program, int(nproc), np.random.randint(0,1000), float(duration)))
            cnt += 1
            if cnt >= 10000:
                break
    trace.sort(key=lambda x: x[2])
    # at most 1000 jobs
    #trace = trace[0:min([len(trace),500])]
    print('Going to simulate %d jobs' % len(trace))
    use_cs = 0
    max_et = 0
    for t in trace:
        cs = t[1]*t[3]
        et = t[2]+t[3]
        if max_et < et:
            max_et = et
        use_cs += cs
    print('used core hour: %.2f, total core hour: %.2f (et=%.2f), occupation = %.2f' % (use_cs/3600, max_et*node_cnt*28/3600, max_et, use_cs/(max_et*node_cnt*28)))
    #print('Job trace:', ', '.join(['%s(%d)' % (x[0], x[2]) for x in trace]))
    header = '%30s\t%8s\t%8s\t%8s\t%8s\t%8s\t%8s\t%8s\t%8s' % ('Algo', 'OCC(%)', 'MAX_TURN', 'USE_CH', 'BUB_CH', 'JOB_WAIT', 'JOB_RUN', 'JOB_TURN', 'CNTorITVL')
    print(header)
    for sim in [simCE, simSS]:
        sim.addTrace(trace)
        sim.addFakeDeamons('sn', node_cnt)
        #sim.addFakeDeamons('bic0', 9408)
        sim.run(alpha=0.9) 
        bs = sim.parse()
        jobcnt = len(sim.parser.records)
        result = '%30s\t%8.2f\t%8.2f\t%8.0f\t%8.0f\t%8.0f\t%8.0f\t%8.0f\t%8.0f' % (
            sim.sched.algo.name, 
            bs['occupation'],
            bs['max_turnaround'], 
            bs['use_corehours'],
            bs['bubble_corehours'],
            sum(bs['jobwaittimes'])/jobcnt,
            sum(bs['jobruntimes'])/jobcnt,
            (sum(bs['jobwaittimes'])+sum(bs['jobruntimes']))/jobcnt,
            node_cnt,)
        print(result)
'''
'''
