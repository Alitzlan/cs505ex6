
"""""""""""""""""""""""""""""""""""""""
FILE: main.py
PURPOSE: main program for every host
AUTHOR: Chi Yang
"""""""""""""""""""""""""""""""""""""""

import sys
import socket
import time
import random
from optparse import OptionParser
from collections import Counter
from os import path

from Raft import *
from Logger import *

myid = None
myname = None
myip = None
myport = None
mysock = None
myaddr = None

myterm = 0
myvote = None
myleader = None

living = None

leaderchange = True

TEST_PING_TIMEOUT = 5

FOLLOWER_TIMEOUT = 0.5
CANDIDATE_TIMEOUT = 0.1
LEADER_TIMEOUT = 0.25

def pingAll():
    global mysock

    if mysock == None:
        logerror(myid,"Socket is not initialized")
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
            logdebug(myid, str(addr)+":"+data)
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
                logerror(myid,msg)
                sys.exit()

    mysock.settimeout(None)

    if len(pending) == 0:
        loginfo(myid,"All ping success")
    else:
        for id in pending:
            loginfo(myid,"{0}:{1} does not respond".format(NAMELOOKUP[ADDRLOOKUP[id][0]],ADDRLOOKUP[id][1]))
        loginfo(myid,"Ping ends with failure")

def initSocket():
    global mysock
    mysock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mysock.bind((myname, myport))

def parseHostfile():
    global HOSTS, IDLOOKUP, ADDRLOOKUP, NAMELOOKUP, myid, myname, myip, myport, myaddr, myterm
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
            logerror(myid,"Not enough args in hostfile")
            sys.exit()
        hostid = int(hostinfo[0])
        hostname = hostinfo[1]

        try:
            hostip = socket.gethostbyname(hostname)
        except socket.gaierror, msg:
            if msg[0] == 11001:
                logerror(myid,"Cannot find ip for host {0}".format(hostname))
                sys.exit()
            else:
                logerror(myid,msg)
                sys.exit()

        # auto-complete port
        if len(hostinfo) < 3:
            logwarning(myid,"Port for host id {0} not assigned, assign {1}".format(hostid, PORT))
            hostport = PORT
        else:
            hostport = int(hostinfo[2])


        # check existence
        if ADDRLOOKUP.has_key(hostid):
            logerror(myid,"Host id {0} already exists, abort".format(hostid))
            sys.exit()
        if IDLOOKUP.has_key((hostip, hostport)):
            logerror(myid,"Host {0}:{1} already exists, abort".format(hostname,hostport))
            sys.exit()
        if not NAMELOOKUP.has_key(hostip):
            NAMELOOKUP[hostip] = hostname

        # find me
        if (myip == hostip or hostip == "127.0.0.1") and hostport == PORT:
            myid, myname, myip, myport = hostid, hostname, hostip, hostport
            myaddr = ( hostip, hostport )
            loginfo(myid,"New Host: {0} {1} {2} *".format(hostid, hostname, hostport))
        else:
            loginfo(myid,"New Host: {0} {1} {2}".format(hostid, hostname, hostport))

        ADDRLOOKUP[hostid] = (hostip, hostport)
        IDLOOKUP[(hostip, hostport)] = hostid

    # check result
    if myid == None:
        logerror(myid,"I am not in the hostfile")
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
        logerror(myid,"Host file not exist")
        sys.exit()

def followerHandle(data, addr):
    global myid, myname, myip, myport, myaddr, mysock, myterm, myvote, myleader, leaderchange
    msg = MessageBody.fromStr(data)
    if msg.type == MessageType.Ping:
        if msg.term > myterm:
            myterm = msg.term
            myleader = msg.id
            leaderchange = True
        elif msg.term == myterm and myleader != msg.id:
            myterm = msg.term
            myleader = msg.id
            leaderchange = True
        if leaderchange:
            print "[{0}] Node {1}: node {2} is elected as new leader.".format(time.strftime("%H:%M:%S",time.localtime()), myid, myleader)
            leaderchange = False
    elif msg.type == MessageType.RequestVote:
        if msg.term > myterm:
            myterm = msg.term
            myleader = msg.id
            mysock.sendto(MessageBody(MessageType.Vote, myterm, msg.id).toString(), addr)
        elif msg.term <= myterm:
            mysock.sendto(MessageBody(MessageType.Vote, myterm, myleader).toString(), addr)
    elif msg.type == MessageType.Vote:
        pass

def followerLoop():
    global myid, myname, myip, myport, myaddr, mysock, myterm, myvote, myleader
    mysock.settimeout(FOLLOWER_TIMEOUT)
    while(True):
        try:
            data, addr = mysock.recvfrom(512)
            logdebug(myid, str(addr)+":"+data)
            followerHandle(data, addr)
        except socket.timeout, msg:
            loginfo(myid,"leader timeout")
            if myleader != None:
                print "[{0}] Node {1}: leader node {2} has crashed.".format(time.strftime("%H:%M:%S",time.localtime()), myid, myleader)
            return RaftState.Candidate
        except socket.error, msg:
            if msg[0] == 10054 and sys.platform == "win32":
                # ignore connection reset because UDP is connection-less
                pass
            else:
                logerror(myid,msg)
                sys.exit()

def candidateLoop():
    global myid, myname, myip, myport, myaddr, mysock, myterm, myvote, myleader, living
    mysock.settimeout(CANDIDATE_TIMEOUT)
    myterm += 1
    myleader = myid
    election_timeout = random.randrange(150, 300) / 1000.
    requestmsg = MessageBody(MessageType.RequestVote, myterm, myid)
    peerid = ADDRLOOKUP.keys()

    # for counting
    if living == None:
        living = set(peerid)
    pending = set(living)
    cnt = Counter()

    start = time.time()
    # broadcast request vote
    for id in peerid:
        mysock.sendto(requestmsg.toString(), ADDRLOOKUP[id])
    while(time.time() - start < election_timeout):
        try:
            data, addr = mysock.recvfrom(512)
            logdebug(myid, str(addr)+":"+data)
            if not IDLOOKUP[addr] in living:
                living.add(IDLOOKUP[addr])
                logdebug(myid, "new live peer <{0}>".format(IDLOOKUP[addr]))
                return RaftState.Candidate
            msg = MessageBody.fromStr(data)
            if msg.type == MessageType.Ping:
                if msg.term >= myterm:
                    return RaftState.Follower
                else:
                    pass
            elif msg.type == MessageType.RequestVote:
                if msg.term > myterm:
                    myterm = msg.term
                    myleader = msg.id
                    mysock.sendto(MessageBody(MessageType.Vote, myterm, msg.id).toString(), addr)
                    return RaftState.Follower
                elif msg.term <= myterm:
                    mysock.sendto(MessageBody(MessageType.Vote, myterm, myleader).toString(), addr)
            elif msg.type == MessageType.Vote:
                if msg.term > myterm:
                    myterm = msg.term
                    myleader = msg.id
                    return RaftState.Follower
                elif msg.term == myterm:
                    if IDLOOKUP[addr] in pending:
                        pending.remove(IDLOOKUP[addr])
                    cnt[msg.id] += 1
                    leading = cnt.most_common(1)
                    if leading[0][1] / len(living) > 0.5:
                        return RaftState.Leader
                else:
                    pass
        except socket.timeout, msg:
            if len(pending):
                for id in pending:
                    logdebug(myid, "no response from <{0}>".format(id))
                    living.remove(id)
                return RaftState.Candidate
            else:
                pass
        except socket.error, msg:
            if msg[0] == 10054 and sys.platform == "win32":
                # ignore connection reset because UDP is connection-less
                pass
            else:
                logerror(myid,msg)
                sys.exit()
    # election timeout
    return RaftState.Candidate

def leaderLoop():
    global myid, myname, myip, myport, myaddr, mysock, myterm, myvote, myleader
    mysock.settimeout(LEADER_TIMEOUT)
    while(True):
        try:
            data, addr = mysock.recvfrom(512)
            logdebug(myid, str(addr)+":"+data)
            msg = MessageBody.fromStr(data)
            if msg.term > myterm:
                followerHandle(data, addr)
                return RaftMessage.Follower
        except socket.timeout, msg:
            #sending heart_beat.
            heartbeat = MessageBody()
            heartbeat.term = myterm
            heartbeat.id = myid
            # broadcast heart_beat
            for id in ADDRLOOKUP.keys():
                #don't send to self
                if(id!=myid):
                    mysock.sendto(heartbeat.toString(), ADDRLOOKUP[id])
        except socket.error, msg:
            if msg[0] == 10054 and sys.platform == "win32":
                # ignore connection reset because UDP is connection-less
                pass
            else:
                logerror(myid,msg)
                sys.exit()

def main():
    global leaderchange
    
    # initialization
    parseOpt()
    initSocket()

    # state loop
    nextState = RaftState.Follower
    while(True):
        if nextState == RaftState.Follower:
            loginfo(myid,"Follower")
            leaderchange = True
            nextState = followerLoop()
        elif nextState == RaftState.Candidate:
            print "[{0}] Node {1}: begin another leader election.".format(time.strftime("%H:%M:%S",time.localtime()), myid)
            loginfo(myid,"Candidate")
            nextState = candidateLoop()
        elif nextState == RaftState.Leader:
            print "[{0}] Node {1}: node {2} is elected as new leader.".format(time.strftime("%H:%M:%S",time.localtime()), myid, myleader)
            loginfo(myid,"Leader")
            nextState = leaderLoop()
        else:
            logerror(myid,"Unrecognized State")
            sys.exit()

if __name__ == "__main__":
    main()
