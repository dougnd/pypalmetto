import unittest
import pypalmetto

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

class palmettoTestMethods(unittest.TestCase):
    def test_palmettoSubmit(self):
        p = pypalmetto.Palmetto(simPBS=True)
        p.printStatus()
        def a():
            print("Hello World")
        j = p.createJob(a)
        j.submit()



if __name__ == '__main__':
    unittest.main()
