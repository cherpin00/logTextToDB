import os
import sys
from subprocess import Popen, PIPE
import argparse

import tailer

if __name__ == "__main__":

    parser=argparse.ArgumentParser()
    tableOption=parser.add_mutually_exclusive_group()
    parser.add_argument("--filein")
    parser.add_argument("--type", choices=["flat","mdaemon_imap"], default="flat") #require filename
    parser.add_argument("--fileout", default=None)
    parser.add_argument("--tail", type=int, default=0) #TODO: 1 does not work here!
    parser.add_argument("--follow", action="store_true", default=False)
    tableOption.add_argument("--append", action="store_true")   #This will be the default tableOption
    tableOption.add_argument("--truncate", action="store_true")
    parser.add_argument("--allow_empty_lines", action="store_true")
    parser.add_argument("--commit", type=int, default=1, choices=range(1,1000000), metavar="[1-1000000]")
    parser.add_argument("--verbose", action="store_true")

    args=parser.parse_args()

    if not args.append and not args.truncate:
        args.append=True

    if args.fileout==None:
        if args.filein==None:
            raise RuntimeError("--fileout param is required when no input filename is specified.  (--filein is empty so stdin is being used)")
        database=args.filein + ".db"
        args.fileout = database
    else:
        database = args.fileout

    append_str = "--append" if args.append else "--truncate"
    follow_str = "--follow" if args.follow else ""
    allow_empty_lines_str = "--allow_empty_lines" if args.allow_empty_lines else ""
    verbose_str = "--verbose" if args.verbose else ""

    log_cmd = f"python main.py --type {args.type} --fileout {args.fileout} {append_str} {allow_empty_lines_str} --commit {args.commit} {verbose_str}" 
    log = Popen(log_cmd, shell=True, stdin=PIPE)

    tail_cmd = f"python pytail.py {args.filein} -n {args.tail} {follow_str}"
    tail = Popen(tail_cmd, shell=True, stdout=log.stdin)

    # tail.wait()
    # log.wait()
