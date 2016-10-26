import unittest
import pypalmetto

class TestStringMethods(unittest.TestCase):
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
        p.runJob(j.runHash)

if __name__ == '__main__':
    unittest.main()
