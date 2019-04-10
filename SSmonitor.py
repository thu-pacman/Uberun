#!/usr/bin/python3
'''
This monitor script is for Intel Xeon E5-2680 v4.
Modification is required for other platforms.
'''
import subprocess
import socket
import numpy as np
from SSconfig import SSConfig as CFG

intvl_s = 5
mon_cmd = 'ssh root@%s perf stat -a -x, -e \
instructions,cycles,\
uncore_ha_0/event=0x01,umask=0x03/,uncore_ha_1/event=0x01,umask=0x03/,uncore_ha_0/event=0x01,umask=0x0C/,uncore_ha_1/event=0x01,umask=0x0C/ \
sleep %d' % (socket.gethostname(), intvl_s)

def get_value():
    p1 = subprocess.run(mon_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, bufsize=1)
    buf1 = [float(s.strip().split(',')[0]) for s in p1.stderr.decode('utf-8').strip().split('\n')]
    # parse output
    v_ips = buf1[0]/intvl_s*1e-9
    v_cps = buf1[1]/intvl_s*1e-9
    v_membw = sum(buf1[-4:])*1e-9*64/intvl_s
    return (v_ips/v_cps, v_membw)

def set_cat(w=CFG.CLUSTER['llcway_per_node']):
    b = ['1']*w
    inv_b = ['1']*(CFG.CLUSTER['llcway_per_node']-w)
    inv_b.extend(['0']*w)
    h = hex(int(''.join(b), 2))
    inv_h = hex(int(''.join(inv_b), 2))
    if w == CFG.CLUSTER['llcway_per_node']:
        pqosEcmd = 'pqos -R'
        subprocess.run(['ssh', 'root@'+socket.gethostname(), pqosEcmd], stdout=subprocess.DEVNULL)
    else:
        pqosEcmd = 'pqos -e "llc:1=%s;llc:2=%s"' % (h, inv_h)
        pqosAcmd = 'pqos -a "llc:1=0-15;llc:2=16-27"'
        subprocess.run(['ssh', 'root@'+socket.gethostname(), pqosEcmd, '&&', pqosAcmd], stdout=subprocess.DEVNULL)
        subprocess.run([CFG.RUN['deploy_path']+'llcflush.sh'], stdout=subprocess.DEVNULL)

#if __name__ == '__main__':
#    #ws = [2,4,8,20,20,8,4,2]
#    ws = [20, 8, 4, 2]
#    cnt = 0
#    while True:
#        #w = np.random.choice(ws)
#        w = ws[cnt]
#        cnt = (cnt+1)%len(ws)
#        # set CAT
#        set_cat(w)
#        # monitor performance
#        for i in range(10):
#            ips, cps, mbw = get_value()
#            if i >= 0:
#                print('%d %d\t%.2f\t%.2f\t%.2f' % (w, i, ips, cps, mbw), flush=True)

if __name__ == '__main__':
    while True:
        for w in CFG.PROF['sample_ways']:
            set_cat(w)
            ipc, mbw = get_value()
            print('%d %.4f %.4f' % (w, ipc, mbw), flush=True)
