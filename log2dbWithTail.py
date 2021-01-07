import os
import sys
from subprocess import Popen, PIPE
import argparse

import tailer

def check_range_commit(arg):
    min=1
    max=1000000
    try:
        value = int(arg)
    except ValueError as err:
       raise argparse.ArgumentTypeError(str(err))

    if value < min or value > max:
        message = f"Expected {min} <= commit <= {max}, got value = {value}"
        raise argparse.ArgumentTypeError(message)

    return value

if __name__ == "__main__":

    # tableOption=parser.add_mutually_exclusive_group()
    # parser.add_argument("--filein")
    # parser.add_argument("--type", choices=["flat","mdaemon_imap"], default="flat") #require filename
    # parser.add_argument("--fileout", default=None)
    # parser.add_argument("--tail", type=int, default=-1) #TODO: 1 does not work here!
    # parser.add_argument("--follow", action="store_true", default=False)
    # tableOption.add_argument("--append", action="store_true")   #This will be the default tableOption
    # tableOption.add_argument("--truncate", action="store_true")
    # parser.add_argument("--allow_empty_lines", action="store_true")
    # parser.add_argument("--commit", type=check_range_commit, default=1, metavar="[1-1000000]")
    # parser.add_argument("--verbose", action="store_true")

    # args=parser.parse_args()

    # if not args.append and not args.truncate:
    #     args.append=True

    # if args.fileout==None:
    #     if args.filein==None:
    #         raise RuntimeError("--fileout param is required when no input filename is specified.  (--filein is empty so stdin is being used)")
    #     database=args.filein + ".db"
    #     args.fileout = database
    # else:
    #     database = args.fileout

    # append_str = "--append" if args.append else "--truncate"
    # follow_str = "--follow" if args.follow else ""
    # allow_empty_lines_str = "--allow_empty_lines" if args.allow_empty_lines else "" #TODO:  There is an empyt line at the begining with "python log2db.py --filein x.log"
    # verbose_str = "--verbose" if args.verbose else ""    



    # log_cmd = f"python main.py --type {args.type} --fileout {args.fileout} {append_str} {allow_empty_lines_str} --commit {args.commit} {verbose_str}" 
    # log = Popen(log_cmd, shell=True, stdin=PIPE)

    mainParser=argparse.ArgumentParser()
    from main import addArgs
    addArgs(mainParser)
    args=mainParser.parse_args()


    args = " ".join(sys.argv[1:])
    log = Popen(f"python log2db.py {args}", shell=True, stdin=PIPE)

    
    # tail_cmd = f"python pytail.py {args.filein} -n {args.tail} {follow_str}"
    # tail = Popen(tail_cmd, shell=True, stdout=log.stdin)
    tail = Popen(f"python pytail.py {args}", shell=True, stdout=log.stdin)
    

    print("log code:", tail.wait())
    print(tail.communicate())

    # log.wait()
