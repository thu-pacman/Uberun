import random
import math
from SSlogger import SSLogger
from SSconfig import SSConfig as CFG
'''
SSScheduler implements the scheduling algorithm
'''
class SSScheduler:
    def __init__(self, algoname, database):
        self.logger = SSLogger('Scheduler')
        if algoname == 'CE':
            self.algo = SSCEAlgorithm()
        elif algoname == 'CS':
            self.algo = SSCSAlgorithm()
        elif algoname == 'SS':
            self.algo = SSSSAlgorithm()
        else:
            self.logger.error('No such algorithm, use CE/CS/SS.')
        self.db = database
        self.logger.succ('Algorithm %s used for resource allocation' % self.algo.name)

    # the algorithm happens here
    # return a dictionary: daemon -> jobspec, and the estimation wall time
    def nextJob(self):
        # no node or no job, cannot schedule
        if len(self.db.pendingJobs) and len(self.db.cluster.nodes):
            jobid = self.db.mostPriorJob()
            # (parallelism, alpha, dict(scale->{time, ipcs, mbws}))
            profile = self.db.getProfile(jobid) 
            # the scheduling algorithm decides the order to try different scales
            # or may only try part of them (CE only tries 1x, E)
            # data structure of candidate is the same with profile
            candidates = self.algo.sortCandidates(profile)
            # self.logger.echo(candidates)
            # try to allocate for each scale, if success, break
            allocation, est = None, None
            for parallelism, scale, mode, alpha, ipcs, mbws, toprofile in candidates:
                N, C, W, B = self.algo.calculateResourceDemand(parallelism, scale, mode, alpha, ipcs, mbws)
                if N <= 0: # N<=0 means not feasible
                    continue
                # resource allocation, if not available (None)
                # allocation is a dict, daemon -> jobspec (see Protocol)
                allocation = self.db.allocateFor(jobid, N, C, W, B, scale, mode, toprofile) 
                if allocation:
                    #self.logger.echo(candidates)
                    est = self.algo.estimate(profile, scale, W)
                    self.db.jobStart(jobid, est)
                    break
            if not allocation:
                self.db.jobStuck(jobid)
            return (allocation, est)
        return (None, None)

'''
The algorithm to decide resource allocation for jobs
All algorithms are implemented for bic cluster, some parameters are hard code
All jobs must be evenly distributed on each node, i.e., P % T == 0
'''

# TODO handle non dividable parallelism

class SSBaseAlgorithm:
    def __init__(self, name):
        self.name = name
        self.total_cores = CFG.CLUSTER['core_per_node']
        self.total_ways = CFG.CLUSTER['llcway_per_node']
        self.total_membw = CFG.CLUSTER['membw_per_node']
        self.logger = SSLogger(name='Algorithm')
    def calculateResourceDemand(self, parallelism, scale, mode, alpha, ipcs, mbws):
        pass
    def sortCandidates(self, profile):
        pass
    def estimate(self, profile, scale, W):
        pass

class SSCEAlgorithm(SSBaseAlgorithm):
    def __init__(self):
        super().__init__('Compact-Exclusive (CE)')
    # return N, C, W, B
    # CE, scale = 1, mode = E, ignore alpha, ipcs, mbws
    def calculateResourceDemand(self, parallelism, scale, mode, alpha, ipcs, mbws):
        N = math.ceil(parallelism/self.total_cores)
        if N == 0:
            return (0,0,0,0)
        C = parallelism // N
        if C * N != parallelism:
            return (0,0,0,0)
        W = self.total_ways
        B = self.total_membw
        return (N, C, W, B)
    # return a list of (parallelism, scale, mode, alpha, ipcs, mbws, toprofile)
    def sortCandidates(self, profile):
        parallelism, _, _ = profile
        candidates = [(parallelism, 1, 'exclusive', 0, [], [], False)]
        return candidates
    # return the estimation runtime according to profile
    def estimate(self, profile, scale, W):
        _, _, ps = profile
        if not ps or 1 not in ps:
            return None
        else:
            return (ps[1]['time'], 1)

class SSCSAlgorithm(SSBaseAlgorithm):
    def __init__(self):
        super().__init__('Compact-Share (CS)')
    # CS, scale = scale, mode = S, ignore alpha, ipcs, mbws
    def calculateResourceDemand(self, parallelism, scale, mode, alpha, ipcs, mbws):
        N = scale * math.ceil(parallelism/self.total_cores)
        if N == 0:
            return (0,0,0,0)
        C = parallelism // N
        if C * N != parallelism:
            return (0,0,0,0)
        W = 0
        B = 0
        return (N, C, W, B)
    # return a list of (parallelism, scale, mode, alpha, ipcs, mbws, toprofile)
    def sortCandidates(self, profile):
        parallelism, _, _ = profile
        candidates = []
        for scale in [1,2,4]:
            candidates.append((parallelism, scale, 'share', 0, [], [], False))
        candidates.sort(key=lambda x: x[1]) # prefer to compact, but can be spread if compact is unavailable
        return candidates
    # return the estimation runtime according to profile
    def estimate(self, profile, scale, W):
        _, _, ps = profile
        if not ps or 1 not in ps:
            return None
        else:
            return (ps[1]['time'], 1)

class SSSSAlgorithm(SSBaseAlgorithm):
    def __init__(self):
        super().__init__('Spread-Share (SS)')
    # SS, use all arguments
    def calculateResourceDemand(self, parallelism, scale, mode, alpha, ipcs, mbws):
        assert(alpha <= 1)
        N = scale * math.ceil(parallelism/self.total_cores)
        # currently SS only handle the case that processes evenly distributed on each node
        if N == 0:
            return (0,0,0,0)
        C = parallelism // N
        if C * N != parallelism:
            return (0,0,0,0)
        if mode == 'exclusive':
            return (N, C, self.total_ways, self.total_membw)
        else:
            # tolerable IPC
            W = self.total_ways
            T_IPC = alpha * max(ipcs) # ipcs[20] should be the max, but not neccessary, due to measure error.
            for i in range(2, self.total_ways+1): # starts from 2 ways
                if ipcs[i] >= T_IPC:
                    W = i
                    break
            B = mbws[W]
            return (N, C, W, B)
    # return a list of (parallelism, scale, mode, alpha, ipcs, mbws, toprofile)
    def sortCandidates(self, profile):
        parallelism, alpha, ps = profile
        candidates = []
        scales = [1,2,4] # NOTE, each socket has 12~28 cores, 4cores saturates the membw, so scale 4 or 8 should be the max scale.
        if ps and 1 in ps: # has profile and scale 1 is in
            #self.logger.warn(ps)
            for scale, prof in ps.items():
                if scale in scales: # only scales of 1,2,4 are considered
                    scales.remove(scale)
                    # the ipc multiplies the speedup of this scale (calibrated by cpu freq factor)
                    speedup = ps[1]['time']/prof['time']/CFG.CLUSTER['cpu_freq_factor'][scale]
                    candidates.append((parallelism, scale, 'share', alpha, [x*speedup for x in prof['ipcs']], prof['mbws'], False))
        for scale in scales: # no profile for those scale, get their profile
            candidates.append((parallelism, scale, 'exclusive', 0, [], [], True)) 
        #self.logger.warn(candidates)
        # if toprofile, sort by scale, largest first; otherwise, sort by execution time, shorter is better.
        candidates.sort(key=lambda x: (1-0.1*x[1]) if x[-1] else ps[x[1]]['time']*CFG.CLUSTER['cpu_freq_factor'][x[1]]) 
        return candidates
    # return the estimation runtime according to profile
    def estimate(self, profile, scale, W):
        _, _, ps = profile
        if not ps or scale not in ps:
            return None
        else:
            ipcs = ps[scale]['ipcs']
            # use ipcs to estimate the performance under W ways
            # use the freq factor to calibrate for shared situation
            est_time = max(ipcs)/ipcs[W]*ps[scale]['time']*CFG.CLUSTER['cpu_freq_factor'][scale]
            est_ratio = est_time/ps[1]['time']
            return (est_time, est_ratio)