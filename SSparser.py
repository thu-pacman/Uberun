#!/usr/bin/python3
import json
import sys
import math
import numpy as np
from datetime import datetime
from SSconfig import SSConfig as CFG
'''
SSParser is used for the statistics and visualization of job records
JOBID     0: 
{"submitTime": 1543986227.61287, "startTime": 1543986227.613871, "finishTime": 1543986227.68582,
"jobattr": {"jobname": "LU16", "framework": "MPI", "parallelism": 16, "alpha": 0.9}, 
"allocation": [["bic01", {"jobid": 0, "jobattr": {"jobname": "LU16", "framework": "MPI", "parallelism": 16, "alpha": 0.9}, 
                          "coremap": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1], 
                          "llcwaymap": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 
                          "leadnode": "bic01", "toprofile": false}]], 
"nodelist": ["bic01"], "NCWB": [1, 16, 20, 120], "scale": 1, "mode": "exclusive", "toprofile": false}
'''
class SSParser:
    def __init__(self):
        self.records = []
    def addRecord(self, record):
        self.records.append(record)
    def addRecords(self, records):
        for r in records:
            self.addRecord(r)
    # select all by default
    def selectRecords(self, selfunc=lambda x: True):
        selRecs = []
        for r in self.records:
            if selfunc(r):
                selRecs.append(r)
        return selRecs
    # get records from file
    def loadFile(self, fname):
        recs = []
        with open(fname, 'r') as fr:
            lines = fr.readlines()
            for line in lines:
                job = json.loads(line[13:])
                rec = {
                    'name': job['jobattr']['jobname'],
                    'jobid': job['allocation'][0][1]['jobid'],
                    'submit': job['submitTime'],
                    'start': job['startTime'],
                    'finish': job['finishTime'],
                    'nproc': job['allocation'][0][1]['jobattr']['parallelism'],
                    'nodelist': job['nodelist']
                    }
                recs.append(rec)
        return recs
    # get records directly from database
    def loadHistory(self, history):
        recs = []
        for _, job in history.items():
            #print(job)
            rec = {
                'name': job['jobattr']['jobname'],
                'jobid': job['allocation'][0][1]['jobid'],
                'submit': job['submitTime'],
                'start': job['startTime'],
                'finish': job['finishTime'],
                'nproc': job['allocation'][0][1]['jobattr']['parallelism'],
                'nodelist': job['nodelist']
                }
            recs.append(rec)
        return recs

    #def getTimestamp(self, s):
        #d = datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')#2018-10-09 07:45:56.068406
        #return d.timestamp()
        #return s
    
    # five important metrics
    # 1. throughput, the reciprocal of node-hour (the sum of wall time on each node)
    # 2. used core hour
    # 3. bubble core hour, the idle core hours inside, exclude the tailing idle cores for CS/SS.
    # 4. job run time, wall time for each individual job
    # 5. job wait time, wait time for each individual job

    def getBasicStats(self, recs):
        def mergeRanges(a):
            b = []
            for begin,end in sorted(a):
                if b and b[-1][1] >= begin - 1: # interval <= 1s, regard as continious
                    b[-1][1] = max(b[-1][1], end)
                else:
                    b.append([begin, end])
            return b
        recs = sorted(recs, key=lambda x: x['jobid'])
        jobids = [] # jobid for each job
        jobruntimes = [] # run second for each job
        jobwaittimes = []  # wait second for each job
        jobusecorehours = [] # core hour for each job
        node_occupied = dict()
        time_bias = min([rec['start'] for rec in recs]) # where the first job starts

        for rec in recs:
            jobids.append(rec['jobid'])
            st, et = rec['start'], rec['finish']
            jobruntimes.append(et - st)
            jobwaittimes.append(st - rec['submit'])
            jobusecorehours.append(jobruntimes[-1]*rec['nproc']/3600) # seconds to hours
            for node in rec['nodelist']:
                if node not in node_occupied:
                    node_occupied[node] = []
                node_occupied[node].append((int(st-time_bias), int(et-time_bias)))
        for node in sorted(node_occupied.keys()):
            node_occupied[node] = mergeRanges(node_occupied[node])
        # now we have node_occupied[node] as a list of non-empty time periods of each node

        # the total 
        max_turnaround = 0
        for _, rs in node_occupied.items():
            max_turnaround = max([max_turnaround, rs[-1][1]])
        max_turnaround /= 3600
        # used (if non empty)
        used_nodehour = 0
        for _, rs in node_occupied.items():
            for r in rs:
                used_nodehour += (r[1] - r[0])
        used_nodehour /= 3600
        total_nodehours = len(node_occupied) * max_turnaround
        occupation = used_nodehour/total_nodehours
        use_corehours = sum(jobusecorehours)
            
        return {'max_turnaround': max_turnaround, 'occupation': occupation*100,
                'use_corehours': use_corehours, 'bubble_corehours': CFG.CLUSTER['core_per_node']*total_nodehours - use_corehours,
                'jobwaittimes': jobwaittimes, 'jobruntimes': jobruntimes }

    def showSchedFig(self, recs):
        import numpy as np
        import matplotlib
        import matplotlib.pyplot as plt
        from PIL import Image
        recs = sorted(recs, key=lambda x: x['jobid'])
        time_bias = min([rec['start'] for rec in recs]) # where the first job starts
        time_end = max([rec['finish'] for rec in recs]) # where the first job starts
        # a list of (nodeidlist, starttime, endtime, timestride, color)
        jobs = []
        n2id = dict()
        nid = 0
        # colors
        colors_heavy = []
        colors_light = []
        for i in range(0, 10):
            rr = i%3*70+100
            gg = int((i%5+1)*0.15*rr)
            bb = i%3*70+100
            colors_heavy.append((rr, gg, 0))
            colors_light.append((0, gg, bb))
        for rec in recs:
            st, et = rec['start']-time_bias, rec['finish']-time_bias
            for node in rec['nodelist']:
                if node not in n2id.keys():
                    n2id[node] = nid 
                    nid += 1
            nlist = [n2id[n] for n in rec['nodelist']]    
            tstr = rec['nproc']//len(nlist)
            if rec['name'] in ['bw-28', 'bw-16']: # heavy
                color = colors_heavy[rec['jobid']%len(colors_heavy)]
            else:
                color = colors_light[rec['jobid']%len(colors_light)]
            jobs.append((nlist, int(st), int(et), int(tstr), color))
        jobs.sort(key=lambda x: x[1])
        #for job in jobs:
        #    print(job)

        core_height = 6
        split_height = 2
        node_height = CFG.CLUSTER['core_per_node']*core_height+split_height
        # PIL accesses images in Cartesian co-ordinates, so it is Image[columns, rows]
        ROWS = nid*node_height
        #COLS = int(math.ceil(time_end-time_bias))+1
        COLS = 3600
        #print(ROWS, COLS)
        img = Image.new('RGB', (COLS, ROWS), 'white') # create a new black image
        pixels = img.load() # create the pixel map

        for job in jobs:
            #print(job)
            for n in job[0]:
                ns = n*node_height
                ne = ns + node_height
                shift = 0
                while pixels[job[1], ns+shift] != (255, 255, 255):
                    shift += core_height
                assert(ns+shift+job[3]*core_height <= ne)
                for j in range(ns+shift, ns+shift+job[3]*core_height): # rows
                    for i in range(job[1], job[2]): # cols
                        pixels[i, j] = job[4]

        img.show()


if __name__ == '__main__':
    def geo_mean(iterable):
        a = np.array(iterable)
        return a.prod()**(1.0/len(a))
# Algorithm CE JobSequence ep-16,hc-28,ts-16,rnn-16,cg-16,ts-16,ep-16,hc-16,ep-16,gan-16,bw-16,bw-28,nw-16,ep-16,hc-28,bfs-16,cg-16,wc-16,hc-16,nw-16
# Compact-Exclusive (CE)      0.90           81.02            0.36              43              38             320             422        job_history_run_CE_20190107-071645.txt
    heavy_jobs = ['bw-16', 'bw-28', 'mg-16', 'lu-16', 'cg-16', 'gan-16', 'ts-16', 'nw-16'] 
    light_jobs = ['hc-16', 'hc-16', 'ep-16', 'rnn-16', 'bfs-16', 'wc-16'] 
    chs = {'mg-16': 97.86, 'cg-16': 665.39, 'lu-16': 1118.29, 'ep-16': 341.19, 'bfs-16': 452.94, 'gan-16': 419.26, 'rnn-16': 313.83,
           'bw-16': 405.83, 'hc-16': 482.33, 'bw-28': 609.38, 'hc-28': 491.5, 'wc-16': 216.83, 'ts-16': 431.89, 'nw-16': 111.94,}
    jsf = dict()
    with open('results.txt', 'r') as fr:
        for line in fr:
            line = line.strip()
            ss = line.split()
            if line.startswith('Algorithm'):
                js = ss[3]
                alg = ss[1]
            else:
                if len(ss) < 10:
                    continue
                hisf = line.split()[-1]
                if js not in jsf:
                    jsf[js] = {'CE': [], 'CS': [], 'SS': []}
                jsf[js][alg].append(hisf)

    with open('toparse.txt', 'r') as fr:
        #jss = sorted(list(set([_.strip() for _ in fr])))
        jss = sorted([_.strip() for _ in fr])
        bad_jss = []
        for js in jss:
            for alg in jsf[js]:
                if len(jsf[js][alg]) == 0:
                    continue
                latest_hisf = max(jsf[js][alg])
                parser = SSParser()
                if latest_hisf.startswith('JobLogs'):
                    parser.addRecords(parser.loadFile(latest_hisf))
                else:
                    parser.addRecords(parser.loadFile('JobLogs/' + latest_hisf))
                recs = sorted(parser.records, key=lambda x: x['jobid'])
                waits = [x['start'] - x['submit'] for x in recs]
                runs = [x['finish'] - x['start'] for x in recs]
                if min(runs) < 20: # An error occurs and the job fails
                    bad_jss.append(js)
                jsf[js][alg] = {'wait': np.array(waits), 'run': np.array(runs)}
                #print(alg, jsf[js][alg])
                #print(jsf[js])
            if len(jsf[js]['CE']) != len(jsf[js]['CS']) or len(jsf[js]['CS']) != len(jsf[js]['SS']):
                bad_jss.append(js)
            if len(jsf[js]['CE']['wait']) != len(jsf[js]['CS']['wait']) or len(jsf[js]['CS']['wait']) != len(jsf[js]['SS']['wait']):
                bad_jss.append(js)
        for js in bad_jss:
            jss.remove(js)
        for js in jss:
            print(js)
        for alg in ['CS', 'SS']:
            violates = []
            heavy_ratios = []
            throughputs = []
            maxs = []
            mins = []
            means = []
            for js in jss:
                heavy_ch, light_ch = 0, 0
                for j in js.split(','):
                    if j in heavy_jobs:
                        heavy_ch += chs[j]*int(j.split('-')[1])
                    else:
                        light_ch += chs[j]*int(j.split('-')[1])
                heavy_ratio = heavy_ch/(heavy_ch+light_ch)

                norm_waits = jsf[js][alg]['wait']/jsf[js]['CE']['wait']
                norm_runs  = jsf[js][alg]['run']/jsf[js]['CE']['run']
                norm_turns = (jsf[js][alg]['wait']+jsf[js][alg]['run'])/(jsf[js]['CE']['wait']+jsf[js]['CE']['run'])
                for c in norm_runs:
                    if c > 1/0.9:
                        violates.append(c)

                heavy_ratios.append('%.4f' % heavy_ratio)
                throughputs.append('%.4f' % (1/(norm_turns.prod()**(1/len(norm_turns)))))
                maxs.append('%.4f' % np.max(norm_runs))
                mins.append('%.4f' % np.min(norm_runs))
                means.append('%.4f' % (norm_runs.prod()**(1/len(norm_runs))))
                #print(' '.join(['%.2f' % _ for _ in jsf[js][alg]['run']]))
                #print(' '.join(['%.2f' % _ for _ in norm_runs]))
                #print('%.4f' % heavy_ratio, ' '.join(['%.2f' % _ for _ in norm_turns]))
            print('ratio = [%s]' % (','.join(heavy_ratios)))
            print('throughput_%s = [%s]' % (alg.lower(), ','.join(throughputs)))
            print('run_max_%s = [%s]' % (alg.lower(), ','.join(maxs)))
            print('run_min_%s = [%s]' % (alg.lower(), ','.join(mins)))
            print('run_mean_%s = [%s]' % (alg.lower(), ','.join(means)))
            print('violates', len(violates), geo_mean(violates), max(violates))
            