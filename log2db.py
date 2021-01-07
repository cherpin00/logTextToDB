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
import datetime

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
	raise EOFError
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
		self.database=database
		self.conn = sqlite3.connect(database)
		self.queue = queue.Queue()

	def connect(self):
		if self.isOpen():
			return conn
		else:
			self.conn = sqlite3.connect(database)
			return self.conn

	def isOpen(self):
		try:
			conn.cursor()
			return True
		except Exception as ex:
			return False

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
		xit=False
		count=0
		maxAttempts=15
		while not xit:
			count+=1
			try:
				c.execute(f"insert into {G_TBL_NAME} ({G_LINE}) VALUES (:{G_LINE})", self.current_info)
				xit=True
			except:
				print(f"Failed to insert, attempt {count} of {maxAttempts}.  pausing 5 seconds...")
				sleep(5)
			if count>=maxAttempts:
				raise RuntimeError(f"Failed to insert after {maxAttempts} attempts")
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

def addArgs(parser):
	tableOption=parser.add_mutually_exclusive_group()
	parser.add_argument("--filein", help="The file to read from.  If not specified, will read from stdin.  If this is not specified, then --fileout must be specified.")
	parser.add_argument("--type", choices=["flat","mdaemon_imap"], default="flat", help="flat:for each line, add to fileout database.  No logic is performed on the line.") #require filename
	parser.add_argument("--processSessions", action="store_true", default=False, help="For each commit batch, call processSessions, which looks for hacking IP's from the committed batch and if found,  outputs json containing IP Address, FirstThreatDate, and LastThreatDate.") #require filename
	parser.add_argument("--fileout", default=None, help="The database to write to.  If the filename specified does not exist, it will be created.  Records will always be inserted into the tblLog table.  If not specified, defaults to --filein with .db extension.  If --filein is not specified, then --fileout is required.")
	parser.add_argument("--tail", type=int, default=-1)
	parser.add_argument("--follow", action="store_true", default=False)
	tableOption.add_argument("--append", action="store_true")   #This will be the default tableOption
	tableOption.add_argument("--truncate", action="store_true")
	parser.add_argument("--allow_empty_lines", action="store_true")
	parser.add_argument("--commit", type=int, default=1, choices=range(1,1000001), metavar="[1-1000000]", help="A DB commit will be issues every --commit lines.  This can speed up processing.")
	parser.add_argument("--verbose", action="store_true")

def sqlGetJsonFromQuery(sql, conn):
	cursor = conn.cursor()
	cursor.execute(sql)
	headers = [x[0] for x in cursor.description]
	rows = cursor.fetchall()

	my_json = []
	for row in rows:
		current_dict = {}
		for index, col in enumerate(headers):
			current_dict[col] = row[index]
		my_json.append(current_dict)
	return my_json

def sqlExecuteNonQuery(sql, conn):
	cursor = conn.cursor()
	cursor.execute(sql)
	conn.commit()

def sqliteExecuteMultipleStatements(sql, conn):
	sql = sql.split(";")
	# print(f"sql is {sql}")
	for s in sql:
		# print(f"s is {s}")
		if s.strip()[:7]=="select ":
			rvalue = sqlGetJsonFromQuery(s, conn)
		else:
			sqlExecuteNonQuery(s, conn)
	# print("done")
	return rvalue

def processNewSessions(conn):
	# print("processing new Sessions...")
	sql = """
	drop table if exists tblNewestSessions;
	create table tblNewestSessions as
		select datetime('now', 'localtime') as now, log_id, srcip
			from vwAuthFailure
			where log_id > ifnull((select log_id from tblSession order by log_id desc limit 1),0)
		;
	--select * from tblNewestSessions;
	insert into tblSession (log_id, username, session, date, protocol, DidLoginSucceed, ReasonForLoginFailure, src, src_ip, src_port, dst, dst_ip, dst_port, line)
		select log_id, logon, sessionID, date, protocol, 0 as DidLoginSucceed, case when account='(none)' then 'vw:Account not found' else 'vw:Invalid password' end as ReasonForLoginFailure
			, null as src, srcip, null as src_port, null as dst, null as dst_ip, null as dst_port, line 
			from vwAuthFailure
			where log_id > ifnull((select log_id from tblSession order by log_id desc limit 1),0)
		;

	drop table if exists tblNewestHackingIP;
	create table tblNewestHackingIP as
		select *
				--, (select count(*) from vwHackingIP)
		from vwHackingIP
		where src_ip in (select srcip from tblNewestSessions)
		;
	--select src_ip, firstDateOfAttack, lastDateOfAttack from tblNewestHackingIP;
	select src_ip, min(firstDateOfAttack) as firstDateOfAttack, max(lastDateOfAttack) as lastDateOfAttack, count(*) as numOccur
		, (select count(*) from vwAuthFailure af where af.srcip=t1.src_ip) as numTotalFailedLogins 
		from tblNewestHackingIP t1 group by src_ip 
		order by numTotalFailedLogins asc
	"""
	start = datetime.datetime.now()
	listHackingIP = sqliteExecuteMultipleStatements(sql, conn)
	finished = datetime.datetime.now() - start

	# print("Finished:", finished)
	if type(listHackingIP) is list:
		if len(listHackingIP)>0:
			for ip in listHackingIP:
				print(ip, flush=True)
		else:
			#We don't have good output - the output should look like JSON
			pass
	# print("Done processing new sessions.")

if __name__ == "__main__":
	# com = os.pipe()
	# tail = threading.Thread(target=tail_process, args=(com, ))
	# log = threading.Thread(target=log_process, args=(com, ))
	# tail.start()
	# log.start()


	# exit()
	parser=argparse.ArgumentParser()

	addArgs(parser)

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
		if args.processSessions:
			truncate_table(database, 'tblSession')


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
			conn.commit()
			# print("Waiting 5 seconds before processsNewSessions....")
			# sleep(1)
			if args.verbose:
				print(f"Line {count},{my_str}")
				# sleep(1)
				# print("Closing connection and waiting 5 seconds....")
				# conn.close()
				# sleep(5)
				# conn=p.connect()
			if args.processSessions:
				processNewSessions(conn)
				conn.commit()

	conn.commit()
	if args.processSessions:
		processNewSessions(conn)
	c = conn.cursor()
	c.execute(f"SELECT * FROM {G_TBL_NAME} where rowid<10")
	print(c.fetchall())
	c.execute(f"SELECT count(*) FROM {G_TBL_NAME}")
	print(c.fetchall())

#TODO:
"""
Add a queue data structure to keep truct of each session.  After we reach a new session we can clense the queue and start over.
"""