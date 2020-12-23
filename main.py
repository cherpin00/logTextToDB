import sys
import os
import multiprocessing as mp
import logging
import sqlite3
import queue

G_TBL_NAME = "tblLog"

G_LINE  = "line"
G_SESSION = "session"
G_CHILD = "child"
G_USER = "user"
G_TIME = "time"
G_transcript_NUMBER = "transcript_number"
G_DATE = "date"

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
        self.logger.info(f"New session {session}")
        self.current_info[G_SESSION] = session
        self.current_info[G_CHILD] = child

    def end_session(self, session):
        self.logger.info(f"Ending session {session}")
        if self.current_info["session"] != session:
            raise RuntimeError(f"Trying to end wrong session. {session}")
        self.current_info[G_SESSION] = None
        self.current_info[G_CHILD] = None

    def new_transcript(self):
        self.logger.info(f"New transcript started")
        current_transcript = self.current_info.get("transcript_number", 0)
        if current_transcript == None:
            current_transcript = 0
        self.current_info[G_transcript_NUMBER] = current_transcript + 1

    def end_transaction(self):
        self.current_info[G_transcript_NUMBER] = 0

    def parse(self, my_str):
        if my_str in ['']:
            return
        INFO = 2
        self.current_info[G_LINE] = my_str
        my_arr = my_str.split(": ")
        try:
            my_arr[INFO]
        except:
            self.logger.error(f"Not parsing line: {my_str}.  Incorrect format.")
            return
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

        date = my_arr[0]
        self.current_info[G_DATE] = date 
        
        c = self.conn.cursor()
        c.execute(f"insert into {G_TBL_NAME} ({G_LINE}, {G_SESSION}, {G_CHILD}, {G_USER}, {G_TIME}, {G_transcript_NUMBER}, {G_DATE}) VALUES (:{G_LINE}, :{G_SESSION}, :{G_CHILD}, :{G_USER}, :{G_TIME}, :{G_transcript_NUMBER}, :{G_DATE})", self.current_info)

        self.conn.commit()

def create_table(database, delete_first = False):
    conn = sqlite3.connect(database)
    c = conn.cursor()

    if delete_first:
        c.execute(f"DROP TABLE {G_TBL_NAME}")

    sql = f"""CREATE TABLE IF NOT EXISTS {G_TBL_NAME} (
            {G_LINE} text,
            {G_SESSION} int,
            {G_CHILD} text,
            {G_USER} text,
            {G_TIME} text,
            {G_transcript_NUMBER} int,
            {G_DATE} text
        )"""
    
    c.execute(sql)

if __name__ == "__main__":

    database = "database.db"
    create_table(database, True)
    
    p = Parser(database)
    count = 0
    while True:
        try:
            my_str = input()
            count += 1
            if count % 1000 == 0:
                print(f"Line {count}")
        except EOFError:
            break
        p.parse(my_str)

    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.execute(f"SELECT * FROM {G_TBL_NAME}")
    print(c.fetchall())
    
#TODO:
"""
Add a queue data structure to keep truct of each session.  After we reach a new session we can clense the queue and start over.
"""