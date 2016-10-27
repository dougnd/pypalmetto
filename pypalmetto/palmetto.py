import os
import dataset
import time
import cloudpickle
import sys
import pickle
from job import Job, JobStatus
import base64
import sh


"""
DB format:
    name: name of job
    time: run time stamp
    status: 
    pbsId: PBS job id
    runHash: hash of the run function and params
    runFunc: pickled run function
    params: pickled params 
    retVal: pickled return value
"""



class Palmetto(object):
    def __init__(self, dbFile=None, simPBS=False):
        homeDir =  os.path.expanduser('~')
        if dbFile:
            self.dbFile = dbFile
        else:
            self.dbFile = homeDir + '/.pypalmetto.db'
        dbStr = 'sqlite:///' + self.dbFile
        db = dataset.connect(dbStr)
        self.jobs = db['jobs']

        self.simPBS = simPBS

    def _dbJobToStr(self, j):
        def timeToStr(t):
            return time.strftime("%m/%d/%Y %H:%M:%S", time.localtime(t))
        def pickleToStr(p):
            if p == '':
                return p
            return str(pickle.loads(base64.b64decode(p)))

        return "{0} {1} {2} {3} {4} funLen: {5} | {6} -> {7}".format(
                j['runHash'], j['name'], timeToStr(j['time']), 
                JobStatus.toStr(j['status']), j['pbsId'],
                len(j['runFunc']),
                pickleToStr(j['params']),
                pickleToStr(j['retVal']))
            
    def printStatus(self):
        print("There are {0} jobs listed in the db".format(len(self.jobs)))
        for j in self.jobs:
            print self._dbJobToStr(j)

    def getJobStatus(self, pbsId):
        if self.simPBS:
            return JobStatus.Completed
        return JobStatus.Error

    def updateDBJobStatuses(self):
        for j in self.jobs:
            if j['status'] != JobStatus.Error and j['status'] != JobStatus.Completed:
                j['status'] = self.getJobStatus(j['pbsId'])
                self.jobs.update(j, ['id'])

    def createJob(self, fun, params=dict(), name='pypalmetto'):
        return Job(fun, self, params, name)

    def submitJob(self,j):
        runHash = j.getHash()
        prevJob = self.jobs.find_one(runHash=runHash)

        dbJob = prevJob if prevJob != None else dict(
                runHash=runHash,
                name=j.getName(),
                params=j.getParamsPickled(),
                runFunc=j.getRunPickled())
        dbJob.update(
                retVal='',
                pbsId='',
                time=time.time(),
                status=JobStatus.Queued)

        if prevJob == None:
            self.jobs.insert(dbJob)
        else:
            self.jobs.update(dbJob, ['id'])


            
    def runJob(self, runHash):
        status = JobStatus.Completed
        job = self.jobs.find_one(runHash=runHash)
        if job == None:
            print("Error, no job with hash: ", runHash)
            return
        print("Running job with hash: ", runHash)
        try:
            runPickeled = base64.b64decode(job['runFunc'])
            paramsPickeled = base64.b64decode(job['params'])
            runFunc = cloudpickle.loads(runPickeled)
            params = pickle.loads(paramsPickeled)
            retPickled = pickle.dumps(runFunc(**params))
            job['retVal'] = base64.b64encode(retPickled)
        except:
            print("Unexpected error when running command:", sys.exc_info()[0])
            status = JobStatus.Error

        job['status'] = status
        self.jobs.update(job, ['id'])



