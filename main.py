"""""""""""""""""""""""""""""""""""""""
from h5py.h5s import NULL
FILE: main.py
PURPOSE: main program for every host
AUTHOR: Chi Yang
"""""""""""""""""""""""""""""""""""""""

import sys
import logging
from optparse import OptionParser
from os import path

from host import Host

def pingAll():
    pass

def initSocket():
    pass

def parseHostfile():
    global HOSTS
    HOSTS = dict()
    hostfile = open(HOSTFILE, "rb")
    for line in hostfile:
        hostinfo = line.rstrip("\r\n").split(" ")
        if len(hostinfo) < 2:
            logging.error("Not enough args in hostfile")
            sys.exit()
        hostid = int(hostinfo[0])
        hostname = hostinfo[1]
        if len(hostinfo) < 3:
            logging.warning("Port for host id {0} not assigned, assign {1}".format(hostid, PORT))
            hostport = PORT
        else:
            hostport = int(hostinfo[2])
        if HOSTS.has_key(hostid):
            logging.error("Host id {0} already exists, abort".format(hostid))
        HOSTS[hostid] = Host(hostid, hostname, hostport)

def parseOpt():
    global PORT, HOSTFILE, MAXCRASH
    usage = "Usage: %prog [<option> <arg>]..."
    parser = OptionParser(usage,add_help_option=False)
    parser.add_option("-p", "--port", dest="port",
                      help="port number for io messages, default 1024",
                      nargs=1, type="int", default=1024)
    parser.add_option("-h", "--hostfile", dest="hostfile",
                      help="filename of host file, default 'hosts'",
                      nargs=1, type="string", default="hosts")
    parser.add_option("-f", "--maxcrash", dest="maxcrash",
                      help="max crashes allowed, default 0",
                      nargs=1, type="int", default=0)
    options, args = parser.parse_args()
    
    PORT = int(options.port)
    HOSTFILE = options.hostfile
    MAXCRASH = int(options.maxcrash)
    
    if path.isfile(HOSTFILE):
        parseHostfile()
    else:
        logging.error("Host file not exist")
        sys.exit()

def main():
    parseOpt()
    pingAll()

if __name__ == "__main__":
    main()