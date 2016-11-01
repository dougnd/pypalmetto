import os
import dataset
import time
import cloudpickle
import sys
import pickle
from job import Job, JobStatus
import base64
import sh
import re


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
    qsubParams: pickled qsubParams
"""



class Palmetto(object):
    def __init__(self, cacheFolder=None, simPBS=False):
        homeDir =  os.path.expanduser('~')
        if cacheFolder:
            self.cacheFolder = cacheFolder
        else:
            self.cacheFolder = homeDir + '/.pypalmetto'
        self.setupCacheFolder()

        dbStr = 'sqlite:///' + self.cacheFolder + '/jobs.db'
        db = dataset.connect(dbStr)
        self.jobs = db['jobs']

        self.simPBS = simPBS

    def setupCacheFolder(self):
        if not os.path.exists(self.cacheFolder):
            os.makedirs(self.cacheFolder)
        if not os.path.exists(self.cacheFolder + '/results'):
            os.makedirs(self.cacheFolder + '/results')

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
        self.updateDBJobStatuses()
        for j in self.jobs:
            print self._dbJobToStr(j)

    def getJobsWithName(self, name):
        jobs = self.jobs.find(name=name)
        jobs = [Job(j, self) for j in jobs]
        return jobs

    def getJobStatusFromHash(self, hash):
        j = self.jobs.find_one(runHash=hash)
        return self.getJobQstatStatus(j['pbsId'])

    def getJobQstatStatus(self, pbsId, qstatOut=None):
        #print("status for {0}".format(pbsId))
        if qstatOut == None:
            qstatOut = str(sh.qstat(x=pbsId))
        #print("out: {0}".format(qstatOut))
        m = re.search('{0}\s+.+\d\s+([QRF])\s+'.format(pbsId), qstatOut)
        if m:
            if m.group(1) == 'R':
                return JobStatus.Running
            elif m.group(1) == 'Q':
                return JobStatus.Queued
            elif m.group(1) == 'F':
                return JobStatus.Error
        return JobStatus.Error

    def updateDBJobStatuses(self):
        user = os.environ['USER']
        qstatOut = str(sh.qstat(u=user))
        for j in self.jobs:
            if j['status'] != JobStatus.Error and j['status'] != JobStatus.Completed:
                j['status'] = self.getJobQstatStatus(j['pbsId'], qstatOut)
                self.jobs.update(j, ['id'])

    def clearDB(self):
        self.jobs.delete()

    def createJob(self, fun, params=dict(), name='pypalmetto', qsubParams=None):
        if qsubParams == None:
            qsubParams = dict(l='select=1:ncpus=1:mem=1gb,walltime=30:00')
        j = dict(runFuncRaw=fun, paramsRaw=params, name=name, qsubParamsRaw=qsubParams)
        return Job(j, self)


    def getJobFileBase(self, j):
        h = j.runHash
        h = h.replace('/', '_').replace('=','-')
        return self.cacheFolder + '/results/' + h
        
    def getJobOutFile(self, j):
        return self.getJobFileBase(j)+'.out'

    def getJobErrFile(self, j):
        return self.getJobFileBase(j)+'.err'

    def submitJob(self,j):
        runHash = j.runHash
        prevJob = self.jobs.find_one(runHash=runHash)

        dbJob = prevJob if prevJob != None else dict(
                runHash=runHash,
                name=j.name,
                params=j.params,
                runFunc=j.runFunc)
        dbJob.update(
                retVal='',
                pbsId='',
                time=time.time(),
                qsubParams=j.qsubParams,
                status=JobStatus.NotSubmitted)

        if prevJob == None:
            self.jobs.insert(dbJob)
        else:
            self.jobs.update(dbJob, ['id'])

        jobStr = """#!/bin/bash
#PBS -N {0}
#PBS -o {1}
#PBS -e {2}

python -m pypalmetto run '{3}'
        """.format(j.name, self.getJobOutFile(j),
                self.getJobErrFile(j), j.runHash)
        #print("About to run qsub with:")
        #print(jobStr)
        qsubParams = j.qsubParamsRaw.copy()
        qsubParams.update({'_in': jobStr})
        pbsId = str(sh.qsub(**qsubParams)).strip()

        dbJob = self.jobs.find_one(runHash=runHash)
        dbJob.update(
                status=JobStatus.Queued,
                pbsId=pbsId)
        self.jobs.update(dbJob, ['id'])

            
    def runJob(self, runHash):
        status = JobStatus.Completed
        job = self.jobs.find_one(runHash=runHash)
        if job == None:
            print("Error, no job with hash: ", runHash)
            return
        print("Running job with hash: ", runHash)
        try:
            runPickled = base64.b64decode(job['runFunc'])
            paramsPickled = base64.b64decode(job['params'])
            runFunc = cloudpickle.loads(runPickled)
            params = pickle.loads(paramsPickled)
            retPickled = pickle.dumps(runFunc(**params))
            job['retVal'] = base64.b64encode(retPickled)
        except:
            print("Unexpected error when running command:", str(sys.exc_info()[0]))
            print(str(sys.exc_info()[0]))
            status = JobStatus.Error
            job['status'] = status
            self.jobs.update(job, ['id'])
            raise

        job['status'] = status
        self.jobs.update(job, ['id'])



