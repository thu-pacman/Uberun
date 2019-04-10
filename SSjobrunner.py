import threading
import subprocess
import os
import numpy
import random
from SSlogger import SSLogger
from SSconfig import SSConfig as CFG

class SSJobBinder(threading.Thread):
    def __init__(self, name, corelist):
        super().__init__()
        self.name = name
        self.corelist = corelist
    def run(self):
        pfind = subprocess.run(['ps', '-a'], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        for line in pfind.stdout:
            ss = line.decode('utf-8').strip().split()
            print(ss)

class SSJobRunner(threading.Thread):
    def __init__(self, hostname, jobspec, name='JobRunner'):
        super().__init__()
        self.jobspec = jobspec
        self.hostname = hostname
        self.logger = SSLogger(name)
        # results for parent
        self.returns = dict()
    # cores[i] = jobid, jobid uses this i-th core
    # ways[i] = jobid, jobid uses this i-th way
    # return a string for CAT 'pqos -s; pqos -a', e.g.
    # sudo pqos -e "llc:1=0xffff0;llc:2=0x0000f"
    # sudo pqos -a "llc:1=0-7;llc:2=8-27" # bic03,bic04
    def getCATString(self, cores, ways):
        self.logger.debug('cores:', cores)
        self.logger.debug('ways:', ways)
        jobids = set(ways) # what jobs are explicitly using LLC
        if -1 in jobids:
            jobids.remove(-1) # -1 means no job
        if len(jobids) == 0: # no CAT, reset. For LLC-unaware policies like CE and CS
            return [['ssh', 'root@' + self.hostname, 'pqos -R']]
        # give the spare ways to jobs
        jobids = list(jobids)
        for i, jid in enumerate(ways):
            if jid == -1:
                ways[i] = jobids[i % len(jobids)]
        # sort the ways with jobid to make each COS use contigious range
        ways.sort()
        # make CAT decision
        cos = []
        for jobid in jobids:
            cos.append({'cores': [], 'ways': []})
            for c, jid in enumerate(cores):
                if jobid == jid:
                    cos[-1]['cores'].append(c)
            for w, jid in enumerate(ways):
                if jobid == jid:
                    cos[-1]['ways'].append(w)
        pqosE = []
        pqosA = []
        for i, c in enumerate(cos):
            s = hex(int(''.join(['1' if x in c['ways'] else '0' for x in range(19,-1,-1)]), 2))
            pqosE.append('llc:%d=%s' % (i+1, s))
            pqosA.append('llc:%d=%s' % (i+1, ','.join(str(x) for x in c['cores'])))
        pqosEcmd = 'pqos -e "%s"' % (';'.join(pqosE))
        pqosAcmd = 'pqos -a "%s"' % (';'.join(pqosA))
        return [['ssh', 'root@' + self.hostname, pqosEcmd], ['ssh', 'root@' + self.hostname, pqosAcmd]]

    # the command use to launch the program
    def getLaunchString(self, jobspec):
        fm = jobspec['jobattr']['framework']
        if fm == 'MPI' or fm == 'Spark':
            if jobspec['leadnode'] != self.hostname:
                return (None, None)
        elif fm == 'TensorFlow':
            assert(len(jobspec['affinity']) == 1)
            pass

        affs = dict()
        for host, corelist in jobspec['affinity'].items():
            affs[host] = []
            for c, jid in enumerate(corelist):
                if jobspec['jobid'] == jid:
                    affs[host].append(str(c))

        envs = dict()
        # running commands
        # format prog-nproc, e.g. mg-16, bfs-32
        prog, nproc = jobspec['jobattr']['jobname'].split('-')
        assert(int(nproc) == jobspec['jobattr']['parallelism'])
        prog = prog.lower()

        # env vars for MPI on bic
        if fm == 'MPI':
            envs['I_MPI_SHM_LMT'] = 'shm'
            envs['I_MPI_DAPL_PROVIDER'] = 'ofa-v2-ib0'
        # env vars for Spark on bic
        if prog in ['mg', 'lu', 'ep', 'cg']: # four NPB programs
            exePath = '%s/%s.D.%s' % (CFG.RUN['exe_path']['npb'], prog, nproc)
            # mpirun -host bic05 -env I_MPI_PIN_PROCESSOR_LIST=1,2,3,4,5,6,7,9 -n 8 ./mg.D.16 : -host bic06 -env I_MPI_PIN_PROCESSOR_LIST=15,16,17,18,19,20,21,22 -n 8 ./mg.D.16
            exeCmd = ['mpirun']
            for host, corelist in affs.items():
                exeCmd.extend(['-host', host, '-env', 'I_MPI_PIN_PROCESSOR_LIST=%s' % ','.join(corelist), '-n', str(len(corelist)), exePath, ':'])
            exeCmd.pop(-1)
        elif prog in ['ts', 'wc', 'nw']: # three spark programs
            spark_master = str(int(jobspec['leadnode'][-2:]))
            exePath = '%s/%s.sh' % (CFG.RUN['exe_path']['spark'], prog)
            exeCmd = [exePath, spark_master]
            for host, corelist in affs.items():
                exeCmd.extend([host, str(len(corelist)), ','.join(corelist)])
            #self.logger.warn(exeCmd)
            self.logger.warn('%s: %s' % (prog, ' '.join(exeCmd)))
        elif prog in ['bfs']: # graph program
            exePath = CFG.RUN['exe_path'][prog]
            exeCmd = ['mpirun']
            for host, corelist in affs.items():
                exeCmd.extend(['-host', host, '-env', 'I_MPI_PIN_PROCESSOR_LIST=%s' % ','.join(corelist), '-n', str(len(corelist)), exePath, '24', '16', ':'])
            exeCmd.pop(-1)
        elif prog in ['hc', 'bw']: # two speccpu programs
            exePath = CFG.RUN['exe_path'][prog]
            exeCmd = ['mpirun']
            for host, corelist in affs.items():
                exeCmd.extend(['-host', host, '-env', 'I_MPI_PIN_PROCESSOR_LIST=%s' % ','.join(corelist), '-n', str(len(corelist)), exePath, ':'])
            exeCmd.pop(-1)
            # TODO, wrap the jobs and move this cd operation to wrapper scripts
            os.chdir(CFG.RUN['exe_dir'][prog])
        elif prog in ['gan', 'rnn']: # two tensorflow programs
            exePath = CFG.RUN['exe_path'][prog]
            assert(len(affs) == 1) # should run on only one node
            for host, corelist in affs.items():
                exeCmd = [exePath, str(len(corelist)), ','.join(corelist)]
            #self.logger.warn(' '.join(exeCmd))
        else:
            assert(False)

        return (envs, exeCmd)
    
    def getProfileString(self, jobspec):
        if not jobspec['toprofile'] or jobspec['leadnode'] != self.hostname:
            return None
        return [CFG.RUN['deploy_path'] + 'SSmonitor.py'] 

    def run(self):
        #self.logger.warn(self.jobspec)
        jobname = self.jobspec['jobattr']['jobname']
        self.logger.debug('Run:', jobname)
        # CAT configuration
        catCmds = self.getCATString(self.jobspec['coremap'], self.jobspec['llcwaymap'])
        for catCmd in catCmds:
            #self.logger.warn('CAT CMD:', ' '.join(catCmd))
            #subprocess.run(catCmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(catCmd, stdout=subprocess.DEVNULL)
        # start the profiler (if needed)
        profCmd = self.getProfileString(self.jobspec)
        if profCmd:
            self.logger.debug('PROF CMD:', ' '.join(profCmd))
            #pPorfiler = subprocess.Popen(profCmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            pPorfiler = subprocess.Popen(profCmd, stdout=subprocess.PIPE)
        # run the executable
        evns, exeCmd = self.getLaunchString(self.jobspec)
        if evns:
            for k, v in evns.items():
                os.environ[k] = v
        if exeCmd:
            self.logger.debug('EXE CMD:', ' '.join(exeCmd))
            pRun = subprocess.run(exeCmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            #pRun = subprocess.run(exeCmd, stdout=subprocess.DEVNULL)#, stderr=subprocess.DEVNULL)
            self.returns['exitcode'] = pRun.returncode
            self.logger.debug('EXE Done:', exeCmd)
            # back to deploy path
            os.chdir(CFG.RUN['deploy_path'])
        # terminate the profiler, sort out the result, and return to daemon to be sent to master
        if profCmd:
            self.logger.debug('check profile results')
            pPorfiler.terminate()
            # llcway ipc mbw
            ipcs, mbws = [], []
            for _ in range(0, 1+CFG.CLUSTER['llcway_per_node']):
                ipcs.append([])
                mbws.append([])
            for line in pPorfiler.stdout:
                ss = line.decode('utf-8').strip().split()
                #self.logger.debug(line)
                if len(ss) != 3:
                    break
                w, ipc, mbw = int(ss[0]), float(ss[1]), float(ss[2])
                ipcs[w].append(ipc)
                mbws[w].append(mbw)
            for w in range(0, 1+CFG.CLUSTER['llcway_per_node']):
                ipcs[w] = numpy.average(ipcs[w]) if len(ipcs[w]) else -1
                mbws[w] = numpy.average(mbws[w]) if len(mbws[w]) else -1
            # linear interpolation
            # self.logger.warn(CFG.PROF['sample_ways'])
            for i in range(0, len(CFG.PROF['sample_ways'])-1):
                cur_w, next_w = CFG.PROF['sample_ways'][i], CFG.PROF['sample_ways'][i+1]
                #self.logger.warn('cur_w %d, next_w %d' % (cur_w, next_w))
                for k in range(min(cur_w, next_w) + 1, max(cur_w, next_w)):
                    #self.logger.warn(k, ipcs)
                    ipcs[k] = ipcs[next_w] + (ipcs[cur_w]-ipcs[next_w])/(cur_w-next_w) * (k-next_w)
                    mbws[k] = mbws[next_w] + (mbws[cur_w]-mbws[next_w])/(cur_w-next_w) * (k-next_w)
            self.logger.warn(ipcs)
                    
            self.returns['ipcs'] = ipcs
            self.returns['mbws'] = mbws
        self.logger.echo('RETURNS', self.returns)

