import numpy as np
from datetime import datetime

def getTimestamp(timestr):
    # 2016-02-02 06:53:00-07:00
    d = datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S%z')
    return d.timestamp()

def genJobs(N=10, light_rato=0.7):
    #progs = ['HH', 'LH', 'HL', 'LL']
    progs = ['lu-16', 'cg-16', 'mg-16', 'ep-16']
    #progs = ['bw-28', 'bw-28', 'bw-28', 'hc-28']
    y = light_rato # the light ratio
    ratio = np.array([1, 1, 1, 3*y/(1-y)]) # ratio for all
    probs = ratio/ratio.sum()
    #print(probs)
    jobs = np.random.choice(progs, size=N, p=probs)
    #print(jobs)
    return jobs

def convert_trinity_trace(light_rato=0.5):
    job_info = [] 
    time_bias = -1
    with open('../../trinity_formatted_release_v0.1.0.csv', 'r') as fr:
        head = True
        for line in fr:
            if head:
                head = False
                continue
            # 0 user_ID, 1 group_ID, 2 submit_time, 3 start_time, 4 dispatch_time, 5 queue_time, 6 end_time,
            # 7 wallclock_limit, 8 job_status, 9 node_count, 10 tasks_requested
            ss = line.strip().split(',')
            if len(ss) < 1:
                break
            if ss[8] != 'JOBEND' or int(ss[10]) == 1:
                continue 
            st = getTimestamp(ss[2]) # submit time
            if time_bias < 0 or st < time_bias:
                time_bias = st
            dr = getTimestamp(ss[6])-getTimestamp(ss[3]) # duration
            nproc = int(ss[10])*28//32 # scale to bic config
            if nproc > 28 and nproc % 28 != 0:
                continue
            # construt the tuple (nproc, st, dr), then combine with prog in {HH, LH, HL, LL} (membw-llc)
            job_info.append((nproc, st, dr))
    job_name = genJobs(len(job_info), light_rato=light_rato) 
    trace = [(jobname, s[0], int(s[1]-time_bias), int(s[2])) for jobname, s in zip(job_name, job_info)]
    with open('trinity_trace.txt', 'w') as fw:
        for t in trace:
            fw.write(','.join([str(x) for x in t]))
            fw.write('\n')
    print('log %d traces' % len(trace))
    
def convert_mustang_trace(light_rato=0.5):
    job_info = [] 
    time_bias = -1
    with open('../../mustang_release_v0.2.0.csv', 'r') as fr:
        head = True
        for line in fr:
            if head:
                head = False
                continue
            # 0 user_ID, 1 group_ID, 2 submit_time, 3 start_time, 4 end_time, 5 wallclock_limit, 6 job_status, 7 node_count, 8 tasks_requested
            ss = line.strip().split(',')
            if len(ss) < 1:
                break
            if ss[6] != 'COMPLETED' or int(ss[8]) == 1:
                continue 
            if len(ss[2])*len(ss[3])*len(ss[4]) == 0:
                continue
            st = getTimestamp(ss[2]) # submit time
            if time_bias < 0 or st < time_bias:
                time_bias = st
            dr = getTimestamp(ss[4])-getTimestamp(ss[3]) # duration
            nproc = int(ss[8])
            if nproc > 24 and nproc % 24 != 0:
                continue
            # construt the tuple (nproc, st, dr), then combine with prog in {HH, LH, HL, LL} (membw-llc)
            job_info.append((nproc, st, dr))
            #if len(job_info) >= 1000:
            #    break
    job_name = genJobs(len(job_info), light_rato=light_rato) 
    trace = [(jobname, s[0], int(s[1]-time_bias), int(s[2])) for jobname, s in zip(job_name, job_info)]
    with open('mustang_trace.txt', 'w') as fw:
        for t in trace:
            fw.write(','.join([str(x) for x in t]))
            fw.write('\n')  
    print('log %d traces' % len(trace))

if __name__ == '__main__':
    heavy_jobs = ['bw-16', 'mg-16', 'lu-16', 'cg-16', 'gan-16', 'ts-16', 'nw-16'] 
    light_jobs = ['hc-16', 'ep-16', 'rnn-16', 'bfs-16', 'wc-16'] 
    spark_jobs = ['nw-16', 'ts-16', 'wc-16']
    jobs = []
    jobs.extend(heavy_jobs)
    jobs.extend(light_jobs)

    for i in range(20):
        js = []
        has_spark = False
        while len(js) < 20:
            j = np.random.choice(jobs)
            if j in spark_jobs:
                if has_spark:
                    continue
                has_spark = True
            js.append(j)
        print(','.join(js))


    exit()

    js = ['bw-16', 'hc-16']
    for x in range(8, 11): # x stone job per 10 jobs
        bunch = []
        fac = 3
        heavy, light = fac*x, fac*(10-x)
        while heavy+light > 0:
            if heavy > 0:
                bunch.append(js[0])
                heavy -= 1
                continue
            if light > 0:
                bunch.append(js[1])
                light -= 1
                continue
        print(','.join(bunch))