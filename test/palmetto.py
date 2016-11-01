import unittest
import pypalmetto
import os

@unittest.skip("skipped")
class skippedPalmettoTestMethods(unittest.TestCase):
    def test_palmettoInit(self):
        p = pypalmetto.Palmetto()

    def test_palmettoStatus(self):
        p = pypalmetto.Palmetto()
        p.printStatus()

    def test_palmettoRun(self):
        p = pypalmetto.Palmetto()
        p.runJob("blksjfaskfj")

    def test_palmettoQueue(self):
        p = pypalmetto.Palmetto()
        def a():
            print("SJFKASJDF!!@#$!SAKDJFA")
        j = p.createJob(a)
        print(j.getStatus())
        j.submit()
        print(j.getStatus())
        p.runJob(j.runHash)
    def test_palmettoSubmit(self):
        p = pypalmetto.Palmetto(simPBS=True)
        p.printStatus()
        def a():
            print("Hello World")
        j = p.createJob(a)
        j.submit()

class palmettoTestSubmitt(unittest.TestCase):
    def test_palmettoQStat(self):
        p = pypalmetto.Palmetto()
        jobs = p.getJobsWithName('py_test')
        if len(jobs) == 0:
            print 'no jobs!'
            def blah():
                os.system('sleep 30')
                print "hello world"
            j = p.createJob(blah, {}, 'py_test')
            print j
            print 'submitting...'
            j.submit()
            print j

        else:
            for j in jobs:
                print j


@unittest.skip("skipped")
class palmettoTestMethods(unittest.TestCase):
    def test_palmettoQStat(self):
        p = pypalmetto.Palmetto()
        s = p.getJobStatusFromHash('p+mlAGtSjm//sxFW1N2gHQ==')
        print(pypalmetto.job.JobStatus.toStr(s))



if __name__ == '__main__':
    unittest.main()
