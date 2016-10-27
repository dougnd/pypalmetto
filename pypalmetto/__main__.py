from palmetto import Palmetto
import argparse


def run(args):
    p = Palmetto()
    p.runJob(args.hash)

def status(args):
    p = Palmetto()
    p.printStatus()

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


