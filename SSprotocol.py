'''
SSProtocol defines a series of message format used in SS
Packing and unpacking data should be done through calling SSprotocal APIs, 
for the consistency between master and worker, especially for futrue modification
'''
class SSProtocol:
    def __init__(self, version='1.0'):
        self.version = version
        self.HEAD_GREETING = 'Greeting'
        self.HEAD_JOBFINISH = 'JobFinish'
        self.HEAD_JOBPROFILE = 'JobProfile'
        self.HEAD_MACHINEINFO = 'MachineProfile' 
        self.HEAD_NEWJOB = 'NewJob'
        self.HEAD_USERCMD = 'UserComand'

    # a greting message send to master when connected
    # role: daemon/master/user
    # hostname: hostname
    def greeting(self, role, hostname):
        return {'head': self.HEAD_GREETING, 'role': role, 'hostname': hostname}
    def isgreeting(self, msg):
        return msg['head'] == self.HEAD_GREETING
    
    # daemon tells master it finishes a job
    # jobid: an integer identifier of a job
    # returns: return values of a job, including exitcode and profile
    def jobfinish(self, jobid, returns):
        return {'head': self.HEAD_JOBFINISH, 'jobid': jobid, 'returns': returns}
    def isjobfinish(self, msg):
        return msg['head'] == self.HEAD_JOBFINISH
    
    # master assign a job specification to daemon
    # jobspec {
    #   jobid: id of the job
    #   jobattr: attributes of the job
    #   coremap: core - jobid mapping, reciever only
    #   llcwaymap: llcway - jobid mapping, reciever only
    #   toprofile: whether to profile the job (ipcs and mbws with diff. llcways), all nodes receive this
    #   leadnode: where to submit this job if only one node is needed (MPI), all nodes receive this
    #   affinity: where to run this job, affinity[hostname] = [0,1,2,3,4...] (cores), leadnode only
    # }
    def newjob(self, jobspec):
        return {'head': self.HEAD_NEWJOB, 'jobspec': jobspec}
    def isnewjob(self, msg):
        return msg['head'] == self.HEAD_NEWJOB
    
    
