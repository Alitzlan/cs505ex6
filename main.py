
"""""""""""""""""""""""""""""""""""""""
FILE: main.py
PURPOSE: main program for every host
AUTHOR: Chi Yang
"""""""""""""""""""""""""""""""""""""""

import sys
import logging
import socket
import time
from optparse import OptionParser
from os import path

from Raft import *

myid = None
myname = None
myip = None
myport = None
mysock = None
myaddr = None

TEST_PING_TIMEOUT = 5

FOLLOWER_TIMEOUT = 1
CANDIDATE_TIMEOUT = 1
LEADER_TIMEOUT = 0.5

def pingAll():
    global mysock
    
    if mysock == None:
        logger.error("Socket is not initialized")
        sys.exit()
        
    # broadcast
    for addr in IDLOOKUP.keys():
        mysock.sendto("Are you there?", addr)
        
    pending = ADDRLOOKUP.keys()
    mysock.settimeout(0.5)
    starttime = time.time()
    while time.time()-starttime < TEST_PING_TIMEOUT:
        try:
            data, addr = mysock.recvfrom(512)
            print addr,":",data
            if data == "Are you there?":
                mysock.sendto("Yes, I am.", addr)
            elif data == "Yes, I am.":
                pending.remove(IDLOOKUP[addr])
        except socket.timeout, msg:
            for id in pending:
                mysock.sendto("Are you there?", ADDRLOOKUP[id])
        except socket.error, msg:
            if msg[0] == 10054 and sys.platform == "win32":
                # ignore connection reset because UDP is connection-less
                pass
            else:
                logger.error(msg)
                sys.exit()
    
    mysock.settimeout(None)
    
    if len(pending) == 0:
        logger.info("All ping success")
    else:
        for id in pending:
            logger.info("{0}:{1} does not respond".format(NAMELOOKUP[ADDRLOOKUP[id][0]],ADDRLOOKUP[id][1]))
        logger.info("Ping ends with failure")

def initSocket():
    global mysock
    mysock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mysock.bind((myname, myport))

def parseHostfile():
    global HOSTS, IDLOOKUP, ADDRLOOKUP, NAMELOOKUP, myid, myname, myip, myport, myaddr
    # get id by addr
    IDLOOKUP = dict()
    # get addr by id
    ADDRLOOKUP = dict()
    # get hostname by ip
    NAMELOOKUP = dict()
    
    myip = socket.gethostbyname(socket.gethostname())
    
    hostfile = open(HOSTFILE, "rb")
    for line in hostfile:
        hostinfo = line.rstrip("\r\n").split(" ")
        
        # check least number of arg
        if len(hostinfo) < 2:
            logger.error("Not enough args in hostfile")
            sys.exit()
        hostid = int(hostinfo[0])
        hostname = hostinfo[1]
        
        try:
            hostip = socket.gethostbyname(hostname)
        except socket.gaierror, msg:
            if msg[0] == 11001:
                logger.error("Cannot find ip for host {0}".format(hostname))
                sys.exit()
            else:
                logger.error(msg)
                sys.exit()
        
        # auto-complete port
        if len(hostinfo) < 3:
            logger.warning("Port for host id {0} not assigned, assign {1}".format(hostid, PORT))
            hostport = PORT
        else:
            hostport = int(hostinfo[2])
            
        
        # check existence
        if ADDRLOOKUP.has_key(hostid):
            logger.error("Host id {0} already exists, abort".format(hostid))
            sys.exit()
        if IDLOOKUP.has_key((hostip, hostport)):
            logger.error("Host {0}:{1} already exists, abort".format(hostname,hostport))
            sys.exit()
        if not NAMELOOKUP.has_key(hostip):
            NAMELOOKUP[hostip] = hostname
            
        # find me
        if (myip == hostip or hostip == "127.0.0.1") and hostport == PORT:
            myid, myname, myip, myport = hostid, hostname, hostip, hostport
            myaddr = ( hostip, hostport )
            logger.info("New Host: {0} {1} {2} *".format(hostid, hostname, hostport))
        else:
            logger.info("New Host: {0} {1} {2}".format(hostid, hostname, hostport))
        
        ADDRLOOKUP[hostid] = (hostip, hostport)
        IDLOOKUP[(hostip, hostport)] = hostid
        
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

def followerLoop():
    mysock.settimeout(FOLLOWER_TIMEOUT)
    while(True):
        try:
            data, addr = mysock.recvfrom(512)
            print addr,":",data
            msg = MessageBody(data)
            if msg.type == MessageType.Ping:
                pass
            elif msg.type == MessageType.RequestVote:
                pass
            elif msg.type == MessageType.Vote:
                pass
        except socket.timeout, msg:
            print "timeout!"
            pass
        except socket.error, msg:
            if msg[0] == 10054 and sys.platform == "win32":
                # ignore connection reset because UDP is connection-less
                pass
            else:
                logger.error(msg)
                sys.exit()
                
def candidateLoop():
    mysock.settimeout(CANDIDATE_TIMEOUT)
    while(True):
        try:
            data, addr = mysock.recvfrom(512)
            print addr,":",data
            msg = MessageBody(data)
            if msg.type == MessageType.Ping:
                pass
            elif msg.type == MessageType.RequestVote:
                pass
            elif msg.type == MessageType.Vote:
                pass
        except socket.timeout, msg:
            pass
        except socket.error, msg:
            if msg[0] == 10054 and sys.platform == "win32":
                # ignore connection reset because UDP is connection-less
                pass
            else:
                logger.error(msg)
                sys.exit()
                
def leaderLoop():
    mysock.settimeout(LEADER_TIMEOUT)
    while(True):
        try:
            data, addr = mysock.recvfrom(512)
            print addr,":",data
            msg = MessageBody(data)
            if msg.type == MessageType.Ping:
                pass
            elif msg.type == MessageType.RequestVote:
                pass
            elif msg.type == MessageType.Vote:
                pass
            #sending heart_beat.
            heartbeat = MessageBody()
            heartbeat.term = myTerm
            heartbeat.id = myid
            # broadcast heart_beat 
            for addr in IDLOOKUP.keys():
                #don't send to self
                if(addr[1]!=myport):
                    mysock.sendto(heartbeat.toString(), addr)
        except socket.timeout, msg:
            pass
        except socket.error, msg:
            if msg[0] == 10054 and sys.platform == "win32":
                # ignore connection reset because UDP is connection-less
                pass
            else:
                logger.error(msg)
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
    
    # state loop
    nextState = RaftState.Follower
    while(True):
        if nextState == RaftState.Follower:
            nextState = followerLoop()
        elif nextState == RaftState.Follower:
            nextState = candidateLoop()
        elif nextState == RaftState.Follower:
            nextState = leaderLoop()
        else:
            logger.error("Unrecognized State")
            sys.exit()

if __name__ == "__main__":
    main()