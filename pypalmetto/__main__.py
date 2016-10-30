from palmetto import Palmetto, JobStatus
import argparse


def run(args):
    p = Palmetto()
    p.runJob(args.hash)

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

def clear(args):
    p = Palmetto()
    p.clearDB()


parser = argparse.ArgumentParser(prog='pypalmetto')
subparsers = parser.add_subparsers()

parser_run = subparsers.add_parser('run', help='run a job from db [should only do this on a node]')
parser_run.add_argument('hash', help='Job hash')
parser_run.set_defaults(func=run)

parser_status = subparsers.add_parser('status', help='Print PyPalmetto DB status')
parser_status.set_defaults(func=status)

parser_clear = subparsers.add_parser('clear', help='Clear DB')
parser_clear.set_defaults(func=clear)

args = parser.parse_args()
args.func(args)


