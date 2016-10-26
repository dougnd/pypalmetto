import cloudpickle
import hashlib
import base64

class JobStatus:
    Completed, Running, Queued, Error, NotSubmitted = range(5)

class Job(object):
    def __init__(self, fRun, palmetto, name):
        self.runPickled = base64.b64encode(cloudpickle.dumps(fRun))
        self.runHash = base64.b64encode(hashlib.md5(
            self.runPickled).digest())
        self.palmetto = palmetto
        self.name = name

    def getHash(self):
        return self.runHash
    def getName(self):
        return self.name
    def getRunPickled(self):
        return self.runPickled

    def getStatus(self):
        prevJob = self.palmetto.jobs.find_one(runHash=self.runHash)
        if prevJob == None:
            return JobStatus.NotSubmitted
        s = prevJob['status']
        if s == JobStatus.Completed or s == JobStatus.Error:
            return s

        prevJob['status'] = self.palmetto.getJobStatus(prevJob['pbsId'])
        self.jobs.update(prevJob, ['id'])
        return prevJob['status']


    def submit(self, force=False):
        s = self.getStatus()
        if s != JobStatus.NotSubmitted and s != JobStatus.Error:
            if not force:
                print("Skipping job")
                return
        self.palmetto.submitJob(self)

        
        




