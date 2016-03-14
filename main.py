"""""""""""""""""""""""""""""""""""""""
FILE: main.py
PURPOSE: main program for every host
AUTHOR: Chi Yang
"""""""""""""""""""""""""""""""""""""""

import sys
import logging
import socket
from optparse import OptionParser
from os import path

from host import Host

myid = None
myname = None
myport = None
mysock = None

def pingAll():
    if mysock == None:
        logger.error("Socket is not initialized")
        sys.exit()
        
    # broadcast
    for host in HOSTS.itervalues():
        mysock.sendto("Are you there?", (host.hostname, host.port))
        
    cnt = 0
    while cnt < len(HOSTS):
        data, addr = mysock.recvfrom(512)
        print addr,":",data
        if data == "Are you there?":
            mysock.sendto("Yes, I am.", addr)
        else:
            cnt += 1

def initSocket():
    global mysock
    mysock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mysock.bind((myname, myport))

def parseHostfile():
    global HOSTS, myid, myname, myport
    HOSTS = dict()
    hostfile = open(HOSTFILE, "rb")
    for line in hostfile:
        hostinfo = line.rstrip("\r\n").split(" ")
        
        # check least number of arg
        if len(hostinfo) < 2:
            logger.error("Not enough args in hostfile")
            sys.exit()
        hostid = int(hostinfo[0])
        hostname = hostinfo[1]
        
        # autocomplete port
        if len(hostinfo) < 3:
            logger.warning("Port for host id {0} not assigned, assign {1}".format(hostid, PORT))
            hostport = PORT
        else:
            hostport = int(hostinfo[2])
            
        
        # check existence
        if HOSTS.has_key(hostid):
            logger.error("Host id {0} already exists, abort".format(hostid))
            
        # find me
        if hostname == socket.gethostname() and hostport == PORT:
            myid, myname, myport = hostid, hostname, hostport
            logger.info("New Host: {0} {1} {2} *".format(hostid, hostname, hostport))
        else:
            logger.info("New Host: {0} {1} {2}".format(hostid, hostname, hostport))
        
        HOSTS[hostid] = Host(hostid, hostname, hostport)
        
    # check result
    if myid == None:
        logger.error("I am not in the hostfile")
        sys.exit()

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
        logger.error("Host file not exist")
        sys.exit()

def main():
    # initial logger
    global logger
    logging.basicConfig()
    logger = logging.getLogger("global")
    logger.setLevel(logging.INFO)
    
    # initialization
    parseOpt()
    initSocket()
    pingAll()
    
    

if __name__ == "__main__":
    main()