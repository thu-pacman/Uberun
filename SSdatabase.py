import json
import os
from datetime import datetime
from SSlogger import SSLogger
from SSconfig import SSConfig as CFG
'''
Database of jobs profile and history, statistics and so on
'''
class SSDatabase:
    def __init__(self, algorithm, simulationClock=None, logToFile=True):
        # the file use to store history, in json format
        self.logToFile = logToFile
        if self.logToFile:
            self.historyFilename = 'JobLogs/%s_%s_%s_%s.txt' % (CFG.DB['history_prefix'], 'sim' if simulationClock else 'run', algorithm, datetime.utcnow().strftime('%Y%m%d-%H%M%S'))
        # the file use to store profile, in json format
        self.profileFilename = CFG.DB['profile_fname']
        # a simulated clock for simulation
        self.simulationClock = simulationClock

        self.jobToReq = dict()
        self.logger = SSLogger('Database')
        self.cluster = SSCluster()
        # Job id, inc by one
        self.jobid = 0
        # jobid -> jobattr
        # jobattr is a dict, e.g., 
        #   'jobname': 'MG16'  use to specify executable binary
        #   'framework': 'MPI' use to build running command
        #   'parallelism': 16 how many cores needed
        #   'alpha': a factor indicates tolerable performance loss
        self.jobidToJobattr = dict()
        # record the resource a job using
        self.jobidToResource = dict()
        # record what daemons a jobid is running on
        self.jobidToDaemons = dict()
        # record the returns of a job
        self.jobidToReturns = dict()
        # a priority criteria used for scheduling
        # jobid -> (current priority, stride, last check timestamp)
        self.jobidToPriority = dict()
        # profile data for programs
        # a program is the executable binary of a job, TODO, more accurate signature
        # currently, we use the jobname 'MG16' as the program signature
        # self.progToProfile['MG16'] is a dict
        # profile[scale factor] = {'time': exectution time, 'ipcs': ipc-ways curve, 'mbws': membw-ways curve}
        self.progToProfile = dict()
        self.loadProfileFromFile()
        # three lists: pending, running, finished
        self.pendingJobs = []
        self.runningJobs = []
        self.completedJobs = []
        # a container to store job history, submit/start/finish/allocation
        # use jobid as key
        self.history = dict()

    def loadProfileFromFile(self):
        # if no such file, create one
        if not os.path.exists(self.profileFilename):
            with open(self.profileFilename, 'w+') as fw:
                fw.write('') 
        # in the file, each line is a dict
        # {'prog': prog, 'scale': scale, 'value': value} 
        # self.progToProfile[prog][scale] = value
        cnt = 0
        with open(self.profileFilename, 'r') as fr:
            for line in fr.readlines():
                if len(line.strip()) == 0:
                    continue
                kv = json.loads(line)
                cnt += 1
                prog, scale, value = kv['prog'], kv['scale'], kv['value']
                if prog not in self.progToProfile:
                    self.progToProfile[prog] = dict()
                self.progToProfile[prog][scale] = value
        self.logger.info('Profile Loaded, %d Entries in total.' % cnt)

    def getTimestampNow(self):
        if self.simulationClock:
            return self.simulationClock.now()
        else:
            return datetime.utcnow().timestamp()

    def addDaemon(self, daemon, hostname):
        self.cluster.addNode(daemon, hostname)
        self.logger.debug('New daemon:', daemon, 'at', hostname)
    
    def addUserJob(self, job):
        jobid = self.jobid
        self.logger.debug('Job added [%d]: %s' % (jobid, job))
        self.jobidToJobattr[jobid] = job
        # add to the pending list
        self.pendingJobs.append(jobid)
        self.jobidToPriority[jobid] = {'value':0, 'stride':CFG.DB['default_stride'], 'lastcheck':self.getTimestampNow()}
        # record the submit time
        self.history[self.jobid] = {'submitTime': self.getTimestampNow(), 'jobattr': job}
        self.jobid += 1
        return jobid
    
    def jobStart(self, jobid, est=-1):
        self.cluster.resourceAlloc(self.jobidToResource[jobid], jobid)
        self.jobidToDaemons[jobid] = [x for x,_,_ in self.jobidToResource[jobid]]
        self.jobidToReturns[jobid] = []
        #self.logger.debug(self.jobidToDaemons)
        self.pendingJobs.remove(jobid)
        self.runningJobs.append(jobid)
        # recover all priority stride
        for _, p in self.jobidToPriority.items():
            p['stride'] = CFG.DB['default_stride']
        self.history[jobid]['startTime'] = self.getTimestampNow()
        self.history[jobid]['estTime'] = est
        self.logger.info('job [%d] (%s) starts, scale %d, resource req:' % (jobid, self.jobidToJobattr[jobid]['jobname'], self.history[jobid]['scale']), 
            self.history[jobid]['NCWB'], ', on nodes:', self.history[jobid]['nodelist'], 'NewProfiling' if self.history[jobid]['toprofile'] else 'InDB')
    
    def jobFinish(self, jobid):
        # record the end time
        self.history[jobid]['finishTime'] = self.getTimestampNow()
        jobtime = int(100*(self.history[jobid]['finishTime'] - self.history[jobid]['startTime']))/100
        # all returns from all daemons
        returns = self.jobidToReturns[jobid]
        # check exitcode, should be 0 for all
        exitcode = 0
        for ret in returns:
            ec = ret.get('exitcode', 0)
            if ec != 0:
                exitcode = ec
                break
        # if has estimation (est_time, est_speedup), return the est_time
        est = self.history[jobid]['estTime'][0] if self.history[jobid]['estTime'] else -1
        if exitcode != 0:
            self.logger.error('job [%d] (%s) finishes after %.2f seconds (%.2f est), with exitcode %d' % 
                (jobid, self.jobidToJobattr[jobid]['jobname'], jobtime, est, exitcode))
        else:
            self.logger.info('job [%d] (%s) finishes after %.2f seconds (%.2f est), with exitcode %d' % 
                (jobid, self.jobidToJobattr[jobid]['jobname'], jobtime, est, exitcode))
        # log the execution record
        if self.logToFile:
            with open(self.historyFilename, 'a') as fw:
                fw.write('JOBID %5d: %s\n' % (jobid, json.dumps(self.history[jobid])))
        # update the profile
        if self.history[jobid]['toprofile']:
            scale = self.history[jobid]['scale']
            prog = self.jobidToJobattr[jobid]['jobname']
            # profile[scale factor] = {'time': exectution time, 'ipcs': ipc-ways curve, 'mbws': membw-ways curve}
            if prog not in self.progToProfile:
                self.progToProfile[prog] = dict()
            # may be repeated by several concurrent profiling runs, only the first one is used
            # ?? or use the last one ??
            if scale not in self.progToProfile[prog]:
                wcnt = CFG.CLUSTER['llcway_per_node'] + 1
                ipcs = [0]*wcnt
                mbws = [0]*wcnt
                ret_cnt = [0]*wcnt
                # average of all daemons
                for ret in returns:
                    if 'ipcs' not in ret:
                        continue
                    for w in range(1, wcnt):
                        ipc, mbw = ret['ipcs'][w], ret['mbws'][w]
                        if ipc > 0 and mbw > 0:
                            ipcs[w] += ipc
                            mbws[w] += mbw
                            ret_cnt[w] += 1
                for w in range(1, wcnt):
                    ipcs[w] = int(10000*ipcs[w]/ret_cnt[w])/10000 if ret_cnt[w] > 0 else -1
                    mbws[w] = int(10000*mbws[w]/ret_cnt[w])/10000 if ret_cnt[w] > 0 else -1
                self.progToProfile[prog][scale] = { 'time': jobtime, 'ipcs': ipcs, 'mbws': mbws }
                # log to file
                with open(self.profileFilename, 'a') as fw:
                    fw.write(json.dumps({'prog': prog, 'scale': scale, 'value': self.progToProfile[prog][scale]}))
                    fw.write('\n')
                self.logger.debug('profile:', self.progToProfile[prog][scale])
        # update other data structures        
        self.cluster.resourceFree(self.jobidToResource[jobid])
        self.jobidToDaemons.pop(jobid)
        self.completedJobs.append(jobid)
        self.runningJobs.remove(jobid)
    
    # should receive a message from each daemon, then the job is really completed.
    def daemonFinishJob(self, dae, jobid, jobreturns):
        self.jobidToDaemons[jobid].remove(dae)
        self.jobidToReturns[jobid].append(jobreturns)
        if len(self.jobidToDaemons[jobid]) == 0:
            self.jobFinish(jobid)
    
    def jobStuck(self, jobid):
        # decrease its priority stride
        self.jobidToPriority[jobid]['stride'] = CFG.DB['slow_stride']
    
    def mostPriorJob(self):
        # update the priority for all jobs
        now = self.getTimestampNow()
        for _, p in self.jobidToPriority.items():
            p['value'] += p['stride'] * (now - p['lastcheck'])
            p['lastcheck'] = now
        # sort pending jobs by their priority (highest first)
        self.pendingJobs.sort(key=lambda x: self.jobidToPriority[x]['value']-x, reverse=True)
        return self.pendingJobs[0]
    
    # return all current profile of the program corresponding to jobid
    def getProfile(self, jobid):
        attr = self.jobidToJobattr[jobid]
        prog = attr['jobname']
        return (attr['parallelism'], attr['alpha'], self.progToProfile.get(prog, None))

    # find allocation (None if not found)
    # scale and mode are for record in history, the NCWB values already imply them
    def allocateFor(self, jobid, N, C, W, B, scale, mode, toprofile):
        # some jobs cannot be scaling out
        if self.jobidToJobattr[jobid]['framework'] == 'TensorFlow': # now we use only single node tf programs
            if scale != 1:
                return None
        # do not allow spread for big jobs. (half machine)
        if N > 32 and scale > 1 and N/scale > 0.5 * len(self.cluster.nodes):
            return None
        # try to allocate resource 
        perNodeReq = {'C':C, 'W':W, 'B':B}
        resourceAllocation = self.cluster.search(N, perNodeReq)
        if resourceAllocation:
            self.jobidToResource[jobid] = resourceAllocation
            #self.logger.debug('Resource can be allocated for', jobid)
            alloc = []
            affinity = dict()
            for daemon, _, _ in resourceAllocation:
                affinity[self.cluster.nodes[daemon]['hostname']] = self.cluster.nodes[daemon]['core']
            nodelist = sorted(affinity.keys())
            leadnode = nodelist[0]
            for daemon, _, _ in resourceAllocation:
                jobspec = {
                    'jobid': jobid,
                    'jobattr': self.jobidToJobattr[jobid],
                    'coremap': self.cluster.nodes[daemon]['core'],
                    'llcwaymap': self.cluster.nodes[daemon]['llcway'],
                    'leadnode': leadnode,
                    'toprofile': toprofile
                }
                #if self.cluster.nodes[daemon]['hostname'] == leadnode:
                    #jobspec['nodelist'] = nodelist
                jobspec['affinity'] = affinity
                alloc.append((daemon, jobspec))
            self.history[jobid]['allocation'] = alloc
            self.history[jobid]['nodelist'] = nodelist
            self.history[jobid]['NCWB'] = (N, C, W, B)
            self.history[jobid]['scale'] = scale
            self.history[jobid]['mode'] = mode
            self.history[jobid]['toprofile'] = toprofile
            return alloc
        else:
            #self.logger.warn('Cannot allocate resource for', jobid)
            return None


'''
Cluster is a collection of nodes
nodes is a dict: daemon ->  node, assume each daemon on each node
nodes[daemon] is a node
node is a dict, keys: hostname, core, llcways, membw, mpi, ml, spark
node[hostname] is a string of hostname
node[core] is a list of core availability, -1 is availablit, other is the jobid on it
node[llcway] is a list of llcways assignment, -1 is not specify, other is the jobid on it
node[membw] is a float, how much memory bandwidth is left
node[mpi,tf,spark] are virtual resources, notes whether the node is able to run this type of jobs
if > 0, yes; if = 0, currenlty no; if < 0, forever no.
'''
class SSCluster:
    def __init__(self):
        self.nodes = dict()
        self.jobToResource = dict()
    
    def __str__(self):
        ans = ''
        for daemon, node in self.nodes.items():
            ans += 'Daemon {0} on Node {1}\n'.format(daemon, node)
        return ans

    # TODO currenlty only consider homogeneous nodes
    def addNode(self, daemon, hostname):
        n = dict()
        n['hostname'] = hostname
        n['core'] = [-1]*CFG.CLUSTER['core_per_node']
        n['llcway'] = [-1]*CFG.CLUSTER['llcway_per_node']
        n['membw'] = CFG.CLUSTER['membw_per_node']
        n['mpi'] = 1
        n['tf'] = 1
        n['spark'] = 1
        self.nodes[daemon] = n

    # check if the node can be use
    # return nodeAlloc and penalty
    def nodeSatisfyReq(self, node, req):
        nosat = (None, None)
        penalty = 0
        nodeAlloc = dict() 
        # ennough core ?
        nodeAlloc['core'] = []
        if node['core'].count(-1) >= req['C']:
            penalty += (CFG.CLUSTER['core_per_node'] - node['core'].count(-1)) # already used cores, add 1 for each used core
            for i, c in enumerate(node['core']):
                if len(nodeAlloc['core']) == req['C']:
                    break
                if c == -1:
                    nodeAlloc['core'].append(i)
        else:
            return nosat
        # enough llc ways ?
        # On current platform, the CAT requires available ways to be contigious
        # However, we will do this in the jobrunner.
        # Here we only record the 'abstract' usage of LLC ways
        nodeAlloc['llcway'] = []
        if node['llcway'].count(-1) >= req['W']:
            penalty += 10*(CFG.CLUSTER['llcway_per_node'] - node['llcway'].count(-1)) # already used ways, add 10 for each used way
            for i, c in enumerate(node['llcway']):
                if len(nodeAlloc['llcway']) == req['W']:
                    break
                if c == -1:
                    nodeAlloc['llcway'].append(i)
        else:
            return nosat
        # enough memory bandwidth ?
        if node['membw'] >= req['B']:
            penalty += (CFG.CLUSTER['membw_per_node'] - node['membw'])/CFG.CLUSTER['membw_per_node'] # already used membw, add 1 for each used GB/s
            nodeAlloc['membw'] = req['B']
        else:
            return nosat
        # special types?
        # TODO special job types
        
        return (nodeAlloc, penalty)

    # mark resource as used
    def resourceAlloc(self, clusterAlloc, jobid):
        for daemon, nodeAlloc, _ in clusterAlloc:
            node = self.nodes[daemon]
            # alloc cores
            for c in nodeAlloc['core']:
                node['core'][c] = jobid
            # alloc llc ways
            for w in nodeAlloc['llcway']:
                node['llcway'][w] = jobid
            # alloc mem bw
            node['membw'] -= nodeAlloc['membw']
    
    def resourceFree(self, clusterAlloc):
        for daemon, nodeAlloc, _ in clusterAlloc:
            node = self.nodes[daemon]
            # free cores
            for c in nodeAlloc['core']:
                node['core'][c] = -1
            # free llc ways
            for w in nodeAlloc['llcway']:
                node['llcway'][w] = -1
            # free mem bw
            node['membw'] += nodeAlloc['membw']

    # search a nodelist that satisfies requriement
    def search(self, N, perNodeReq):
        ans = []
        zero_penalty = 0
        for daemon, node in self.nodes.items():
            nodeAlloc, penalty = self.nodeSatisfyReq(node, perNodeReq)
            if nodeAlloc:
                ans.append((daemon, nodeAlloc, penalty))
                if penalty == 0:
                    zero_penalty += 1
            if zero_penalty >= N:
                break
        if len(ans) >= N:
            # use the penalty to sort and use the least penalty nodes
            ans.sort(key=lambda x: x[2])
            clusterAlloc = ans[0:N]
            return clusterAlloc
        else:
            return None


