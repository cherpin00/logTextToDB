import os
import signal
import sys
import multiprocessing as mp
import threading
import subprocess
import logging
import sqlite3
import queue
import argparse
from time import sleep

import tailer

import pytail

G_TBL_NAME = "tblLog"

G_LINE  = "line"
G_SESSION = "session"
G_CHILD = "child"
G_USER = "user"
G_TIME = "time"
G_transcript_NUMBER = "transcript_number"
G_DATE = "date"

G_NUM_ROWS = 0
G_SQL = ""

def signal_handler(sig, frame):
    print("CONTROL+C was pressed - End of File")
    # print(sig)
    # print(frame)
    # sys.exit(0)

class Parser:
    def __init__(self, database):
        self.logger = logging.getLogger("parser")
        self.logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        self.logger.addHandler(ch)
        self.current_info = {
            G_LINE : None,
            G_SESSION : None,
            G_CHILD : None,
            G_USER : None,
            G_TIME : None,
            G_transcript_NUMBER : None,
        }
        self.conn = sqlite3.connect(database)
        self.queue = queue.Queue()

    def __del__(self):
        del self.conn

    def new_session(self, session, child):
        # self.logger.info(f"New session {session}")
        self.current_info[G_SESSION] = session
        self.current_info[G_CHILD] = child

    def end_session(self, session):
        # self.logger.info(f"Ending session {session}")
        if self.current_info["session"] != session:
            raise RuntimeError(f"Trying to end wrong session. {session}")
        self.current_info[G_SESSION] = None
        self.current_info[G_CHILD] = None

    def new_transcript(self):
        # self.logger.info(f"New transcript started")
        current_transcript = self.current_info.get(G_transcript_NUMBER, 0)
        if current_transcript == None:
            current_transcript = 0
        self.current_info[G_transcript_NUMBER] = current_transcript + 1

    def end_transaction(self):
        self.current_info[G_transcript_NUMBER] = 0


    isInFetch = False
    numFetches = 0
    lastFetch = ""

    def parse_flat(self, line, isCommit=False, isAllowEmptyLines=False):
        if my_str in ['']:
            if not isAllowEmptyLines:
                return
        
        self.current_info[G_LINE]=line

        c = self.conn.cursor()
        c.execute(f"insert into {G_TBL_NAME} ({G_LINE}) VALUES (:{G_LINE})", self.current_info)
        # c.execute(f"insert into {G_TBL_NAME} ({G_LINE}, {G_SESSION}, {G_CHILD}, {G_USER}, {G_TIME}, {G_transcript_NUMBER}, {G_DATE}) VALUES (:{G_LINE}, :{G_SESSION}, :{G_CHILD}, :{G_USER}, :{G_TIME}, :{G_transcript_NUMBER}, :{G_DATE})", self.current_info)

        if isCommit:
            self.conn.commit()

    def parse_mdaemon_imap(self, my_str):
        if my_str in ['']:
            return
        INFO = 2
        self.current_info[G_LINE] = my_str
        my_arr = my_str.split(": ")
        try:
            my_arr[INFO]    #todo: what does this do?  Document or do it better.  Check the length of my_array rather than force an exception....
        except:
            self.logger.error(f"Not parsing line: {my_str}.  Incorrect format.")
            return

        if self.isInFetch:
            if " FETCH (UID " in my_arr[INFO]:
                self.numFetches += 1
                self.lastFetch = self.current_info[G_LINE]
                return #Don't log each Fetch, just summarize them
            else:
                # self.logger.error(f"Counted Fetches, total={self.numFetches}")
                self.current_info[G_LINE] = f"Last Fetch:{self.lastFetch}, ---Fetch count:{self.numFetches}"
                self.numFetches = 0
                self.isInFetch=False
                #Todo: If last line is processes and still isInFetch, then end Fetch.
        else:
            if " FETCH (UID " in my_arr[INFO]:
                self.lastFetch = self.current_info[G_LINE]
                self.isInFetch=True
                self.numFetches = 1
                return
            else:
                pass

        if "--------- Partial transcript" in my_arr[INFO]:
            self.new_transcript()
        elif my_arr[INFO].split(" ")[0] == "Session":
            smaller_arr = my_arr[INFO].split(";")
            session = smaller_arr[0].split(" ")[1]
            child = smaller_arr[1].strip().split(" ")[1]
            self.new_session(session, child)
        elif "---------- End partial transcript." in my_arr[INFO]:
            self.end_transaction()
        else:
            pass

        date = my_arr[0][4:]  #expecting e.g. "Sat 2020-12-19 00:00:56.086", this removes the day of week so we get a sqlite3 datetime text
        self.current_info[G_DATE] = date 
        
        c = self.conn.cursor()
        c.execute(f"insert into {G_TBL_NAME} ({G_LINE}, {G_SESSION}, {G_CHILD}, {G_USER}, {G_TIME}, {G_transcript_NUMBER}, {G_DATE}) VALUES (:{G_LINE}, :{G_SESSION}, :{G_CHILD}, :{G_USER}, :{G_TIME}, :{G_transcript_NUMBER}, :{G_DATE})", self.current_info)

        # self.conn.commit()

def truncate_table(database, tablename):
    conn = sqlite3.connect(database)
    with conn as cursor:    #will autocommit
        cursor.execute(f"delete from {tablename}")  #with no where clause, this is a truncate

def create_table(database, delete_first = False):
    conn = sqlite3.connect(database)
    c = conn.cursor()

    if delete_first:
        c.execute(f"DROP TABLE if exists {G_TBL_NAME}")

    sql = f"""CREATE TABLE IF NOT EXISTS {G_TBL_NAME} (
            id integer primary key autoincrement,
            {G_LINE} text,
            {G_SESSION} int,
            {G_CHILD} text,
            {G_USER} text,
            {G_TIME} text,
            {G_transcript_NUMBER} int,
            {G_DATE} text
        );"""
    
    c.execute(sql)
    c.execute("""CREATE INDEX if not exists "tblLog_session" ON "tblLog" (
        	"session"
        );""")
    c.execute("""
        CREATE INDEX if not exists "tblLog_date" ON "tblLog" (
        	"date"	ASC
        );"""
        )
    conn.commit()


def tail_process(write_pipe):
    # sys.stdout=pipe
    pytail.main("x.log", 5, False, out=os.fdopen(write_pipe[1], "w"))

def log_process(read_pipe):
    # while True:
    f = os.fdopen(read_pipe[0], 'r')

if __name__ == "__main__":
    # com = os.pipe()
    # tail = threading.Thread(target=tail_process, args=(com, ))
    # log = threading.Thread(target=log_process, args=(com, ))
    # tail.start()
    # log.start()



    # exit()
    parser=argparse.ArgumentParser()
    tableOption=parser.add_mutually_exclusive_group()
    parser.add_argument("--filein")
    parser.add_argument("--type", choices=["flat","mdaemon_imap"], default="flat") #require filename
    parser.add_argument("--fileout", default=None)
    parser.add_argument("--tail", type=int, default=-1)
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
    else:
        database = args.fileout

    print("args",args)
    print("Database:",database)

    # database = ":memory:" //todo: Why doesn't in memory database work.  It returns no such table tblLog even after tblLog is created
    create_table(database, delete_first=False)  #only create the table if it doesn't exist
    if args.truncate:
        truncate_table(database, G_TBL_NAME)


    # create_table(database, False)

    p = Parser(database)
    conn=p.conn
    # conn.setAutoCommit(False)
    if args.filein is not None:
        f = open(args.filein, mode="r")
        sys.stdin = f

    count = 0

    signal.signal(signal.SIGINT, signal_handler)

    while True:
        try:
            my_str = input()
            # my_str = "Sat 2020-12-19 03:44:01.396: 05: Accepting IMAP connection from 185.36.248.110:39462 to 192.168.1.25:143"
        except EOFError:
            break
        
        if args.type=="flat":
            p.parse_flat(my_str, isAllowEmptyLines=args.allow_empty_lines)
        elif args.type=="mdaemon_imap":
            p.parse_mdaemon_imap(my_str)
        else:
            raise RuntimeError("invalid file type specified.  See --help")

        count += 1
        if count % args.commit == 0:
            if args.verbose:
                print(f"Line {count},{my_str}")
            conn.commit()

    conn.commit()
    c = conn.cursor()
    c.execute(f"SELECT * FROM {G_TBL_NAME}")
    print(c.fetchall())
    c.execute(f"SELECT count(*) FROM {G_TBL_NAME}")
    print(c.fetchall())

#TODO:
"""
Add a queue data structure to keep truct of each session.  After we reach a new session we can clense the queue and start over.
"""