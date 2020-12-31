#!/usr/bin/env python3

import argparse
import os
import shutil
import sys

import tailer

def get_input_args():
    """
    Retrieves and parses the command line arguments created and defined using the argparse module.
    :return: parse_args() -data structure that stores the command line arguments object
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("file_path", help="the path to the file to tail")
    parser.add_argument("-n", "--num-lines", help="the number of lines to print", default=0)
    parser.add_argument("--follow", action="store_true", default=False)

    # args = parser.parse_args(["input.txt", "-n", "5"])
    # args = parser.parse_args(["x_unix.log", "-n", "5"])
    args = parser.parse_args()

    return args

def myRead(f):
    #Will usually read one char and f.tell() will be increased by 1.
    #However, to handle various types of line endings, if \n is read and the prior char is a \r, then f.tell() will not change (so as to skip the \r)
    #This was written to work with MAC(\r) but has not been tested for MAC.  TODO:Test files with MAC line endings(\r)
    n=1
    currentPos=f.tell()
    c=f.read(n)
    if c==b"\n": #handle various line endings.  This should work for linux(\n), windows(\r\n), and MAC(\r).
        #if line ends in \n, we check if this is a Windows line(\r\n).  If so, we skip the \r
        f.seek(f.tell()-2, os.SEEK_SET)
        if f.read(1)!=b"\r":
            f.seek(f.tell()+1, os.SEEK_SET) #skip \r
    if c==b"\r": #this will handle MAC(\r).  we can always just check for \n
        c=b"\n"
    tellAfter=f.tell()
    if tellAfter-currentPos>n:
        f.seek(currentPos+n, os.SEEK_SET)
    tellAfter2=f.tell()
    return c

def tail_from(f, num_lines):
    """
    Sets the file's current position to the point from which to print the tail output, given the number of lines to print.
    It is assumed the file has already been opened with mode 'r'.
    :param f: file
    :param num_lines: the number of lines to tail
    :return: nothing; instead sets the file cursor to the point to tail from
    """
    # Seek to end of file, reading from here will give us '', so we'll start our loop by backing up 1 char
    f.seek(0, os.SEEK_END)

    # While there is still a positive number of lines to tail and we're not at the beginning of the file,
    # move the cursor back one character, read that character and check to see if it is a line separator.
    while num_lines > 0 and f.tell() > 0:
        f.seek(f.tell() - 1, os.SEEK_SET)
        ch = myRead(f)
        # ch=f.read(1)

        # if ch == os.linesep:
        if ch == b"\n":
            num_lines -= 1
        # if num_lines > 0:
            # Back up an additional character because reading moved our cursor forward (1 forward, 2 back each iteration)
        f.seek(f.tell() - 1, os.SEEK_SET)
    
    ch=f.read(1)
    if ch==b"\r":    #cursor may be sitting between \r\n or on a \r or \n.  This will guarantee we are not sitting on a new line character.
        #Todo: Test this for MAC(\r).  Already tested for Unix and DOS
        if f.read(1) != b"\n":
            f.seek(f.tell()-1, os.SEEK_SET)
    else:
        if ch!=b"\n":
            f.seek(f.tell()-1, os.SEEK_SET) #didn't find a newline so go back
    
def dos2mac(filein, fileout):
    with open(filein, "r") as f:
        lines = f.readlines()
    with open(fileout, "wb") as f:
        for line in lines:
            line = line.strip() + "\r"
            f.write(line.encode())
    with open(fileout, "rb") as f:
        print(f.read())

def dos2mac2(filein, fileout):
    with open(filein, "rb") as f:
        lines = f.read().decode()
    with open(fileout, "wb") as f:
        f.write(lines.replace("\r\n", "\r").encode())
    with open(fileout, "rb") as f:
        print(f.read())

def main():
    args = get_input_args()

    full_file_path = os.path.join(os.path.join(os.path.abspath(os.path.dirname(__file__)), args.file_path))

    with open(full_file_path, 'br') as f:   #read as bytes so python doesn't auto-convert line-endings
        tail_from(f, int(args.num_lines))
        # shutil.copyfileobj(f, sys.stdout)
        line1=""
        for line in f:
            line1=line
            print(line.decode().replace("\r\n","\n"), end="", flush=True)
        if line1.decode()[-1]!="\n" and line1.decode()[-1]!="\r":
            print(flush=True)

    if args.follow:
        for line in tailer.follow(open(full_file_path),.1):
            print(line, flush=True)


if __name__ == "__main__":
    # dos2mac("x.log", "x_mac.log")
    # dos2mac2("x.log", "x_mac.log")
    main()