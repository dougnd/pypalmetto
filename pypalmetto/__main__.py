from palmetto import Palmetto, JobStatus, Job
import argparse
import time


def run(args):
    p = Palmetto()
    p.runJob(args.hash)


def printJobs(jobs):
        from tabulate import tabulate
        def timeToStr(t):
            return time.strftime("%m/%d/%Y %H:%M:%S", time.localtime(t))
        print tabulate([[
            j['id'], j['pbsId'],
            JobStatus.toStr(j['status']),
            j['runHash'], j['name'], 
            timeToStr(j['time'])] for j in jobs],
            headers=['ID', 'PBS ID', 'Status', 'Hash', 'Name', 'Submit Time'])

def status(args):
    p = Palmetto()
    print("Updating status of running jobs...")
    p.updateDBJobStatuses()
    print("There are {0} jobs listed in the db".format(len(p.jobs)))

    rJobs = (p.jobs.count(status=JobStatus.Running))
    if rJobs > 0:
        print("There are {0} jobs running".format(rJobs))

    qJobs = (p.jobs.count(status=JobStatus.Queued))
    if qJobs > 0:
        print("There are {0} jobs queued".format(qJobs))

    cJobs = (p.jobs.count(status=JobStatus.Completed))
    if cJobs > 0:
        print("There are {0} jobs completed".format(cJobs))

    eJobs = (p.jobs.count(status=JobStatus.Error))
    if eJobs > 0:
        print("There are {0} jobs with error".format(eJobs))
        if args.verbose_errors:
            for j in p.jobs.find(status=JobStatus.Error):
                print(Job(j, p))

    if args.name:
        print "Jobs with name {0}:".format(args.name)
        dbJobs = p.jobs.find(name=args.name)
        printJobs(dbJobs)
    if args.show_all:
        print "All Jobs:"
        printJobs(p.jobs.all())


def clear(args):
    p = Palmetto()
    if args.clear_all:
        print "Clearing all!"
        p.clearDB()

    if args.name:
        print "Clearing jobs with name {0}!!".format(args.name)
        p.jobs.delete(name=args.name)



parser = argparse.ArgumentParser(prog='pypalmetto')
subparsers = parser.add_subparsers()

parser_run = subparsers.add_parser('run', help='run a job from db [should only do this on a node]')
parser_run.add_argument('hash', help='Job hash')
parser_run.set_defaults(func=run)

parser_status = subparsers.add_parser('status', help='Print PyPalmetto DB status')
parser_status.set_defaults(func=status)
parser_status.add_argument('-e', '--verbose-errors', action='store_true')
parser_status.add_argument('-n', '--name', help='Show status of jobs with name')
parser_status.add_argument('-a', '--show-all', action='store_true', help='Show that status of all jobs!')


parser_clear = subparsers.add_parser('clear', help='Clear DB')
parser_clear.set_defaults(func=clear)
parser_clear.add_argument('-n', '--name', help='Clear DB of jobs of a particular name')
parser_clear.add_argument('-a', '--clear-all', action='store_true')

args = parser.parse_args()
args.func(args)


