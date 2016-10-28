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
    def __init__(self, fRun, palmetto, params, name):
        self.runPickled = base64.b64encode(cloudpickle.dumps(fRun))
        self.params = params
        self.paramsPickled = base64.b64encode(pickle.dumps(params))
        self.runHash = base64.b64encode(hashlib.md5(
            self.runPickled + self.paramsPickled).digest())
        self.palmetto = palmetto
        self.name = name
        if not isinstance(params, dict):
            raise InputError("Job parameters are not a dict type!")

    def getHash(self):
        return self.runHash
    def getName(self):
        return self.name
    def getRunPickled(self):
        return self.runPickled
    def getParamsPickled(self):
        return self.paramsPickled
    def getParams(self):
        return self.params

    def getStatus(self):
        prevJob = self.palmetto.jobs.find_one(runHash=self.runHash)
        if prevJob == None:
            return JobStatus.NotSubmitted
        s = prevJob['status']
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
        params = pickle.loads(paramsPickled)
        return runFunc(**params)

        
        




