import os
import dataset
import time
import cloudpickle
import sys
import pickle
from job import Job, JobStatus
import base64


"""
DB format:
    name: name of job
    time: run time stamp
    status: 
    pbsId: PBS job id
    runHash: hash of the run function
    runFunc: pickled run function
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
            
    def printStatus(self):
        print("There are {0} jobs listed in the db".format(len(self.jobs)))
        for j in self.jobs:
            print j

    def getJobStatus(self, pbsId):
        if self.simPBS:
            return JobStatus.Completed
        return JobStatus.Error

    def updateDBJobStatuses(self):
        for j in self.jobs:
            if j['status'] != JobStatus.Error and j['status'] != JobStatus.Completed:
                j['status'] = self.getJobStatus(j['pbsId'])
                self.jobs.update(j, ['id'])

    def createJob(self, fun, name='pypalmetto'):
        return Job(fun, self, name)

    def submitJob(self,j):
        runHash = j.getHash()
        prevJob = self.jobs.find_one(runHash=runHash)

        dbJob = prevJob if prevJob != None else dict(
                runHash=runHash,
                name=j.getName(),
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
            runFunc = cloudpickle.loads(runPickeled)
            job['retVal'] = pickle.dumps(runFunc())
        except:
            print("Unexpected error when running command:", sys.exc_info()[0])
            status = JobStatus.Error

        job['status'] = status
        self.jobs.update(job, ['id'])



