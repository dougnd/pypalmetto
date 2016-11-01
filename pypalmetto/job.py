import cloudpickle
import pickle
import hashlib
import base64

class JobStatus:
    Completed, Running, Queued, Error, NotSubmitted = range(5)
    strs = ['Completed', 'Running', 'Queued', 'Error', 'NotSubmitted']
    @staticmethod
    def toStr(s):
        return JobStatus.strs[s]


class Job(object):
    def __init__(self, jobDict, palmetto):
        if 'runFuncRaw' in jobDict and 'paramsRaw' in jobDict:
            self.runFunc = base64.b64encode(
                    cloudpickle.dumps(jobDict['runFuncRaw']))
            self.params = base64.b64encode(
                    pickle.dumps(jobDict['paramsRaw']))
            self.runHash = base64.b64encode(hashlib.md5(
                    self.params).digest())
        if 'qsubParamsRaw' in jobDict:
            self.qsubParams= base64.b64encode(
                    pickle.dumps(jobDict['qsubParamsRaw']))
        if 'qsubParams' in jobDict:
            self.qsubParamsRaw = pickle.loads(
                    base64.b64decode(jobDict['qsubParams']))

        self.__dict__.update(jobDict)
        self.palmetto = palmetto

    def __str__(self):
        return '<Job: name={0}, hash={1}, status={2}>'.format(
                self.name, self.runHash, 
                JobStatus.toStr(self.getStatus()))

    def decode(self, val):
        return pickle.loads(base64.b64decode(val))


    def getStatus(self, fast=False):
        prevJob = self.palmetto.jobs.find_one(runHash=self.runHash)
        if prevJob == None:
            return JobStatus.NotSubmitted
        s = prevJob['status']

        if fast:
            return s

        if s != JobStatus.Running and s != JobStatus.Queued:
            return s

        prevJob['status'] = self.palmetto.getJobQstatStatus(prevJob['pbsId'])
        self.palmetto.jobs.update(prevJob, ['id'])
        return prevJob['status']


    def submit(self, force=False):
        s = self.getStatus()
        #print("Status: {0}".format(JobStatus.toStr(s)))
        if s != JobStatus.NotSubmitted and s != JobStatus.Error:
            if not force:
                print("Skipping job")
                return
        self.palmetto.submitJob(self)

    def executeLocal(self):
        runPickled = base64.b64decode(self.runPickled)
        paramsPickled = base64.b64decode(self.paramsPickled)
        runFunc = cloudpickle.loads(runPickled)
        params = pickle.loads(self.params)
        return runFunc(**params)

        
        




