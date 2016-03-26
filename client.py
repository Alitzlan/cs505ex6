import sys 
import socket
import time
import logging
mysock = None
wifeHost = 'wifeHost'
def parseHostfile():
    global HOSTS, IDLOOKUP, ADDRLOOKUP, NAMELOOKUP
    # get id by addr
    IDLOOKUP = dict()
    # get addr by id
    ADDRLOOKUP = dict()
    # get hostname by ip
    NAMELOOKUP = dict()
    
    hostfile = open('wifeHost', "rb")
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
            logger.warning("Port for host id {0} not assigned, assign {1}".format(hostid, 2024))
            hostport = 2024
        else:
            hostport = int(hostinfo[2])
        
        # check existence
        if ADDRLOOKUP.has_key(hostid):
            logger.error("Host id {0} already exists, abort".format(hostid))
            sys.exit()
        if IDLOOKUP.has_key((hostip, hostport)):
            logger.error("Host {0}:{1} already exists, abort".format(hostname, hostport))
            sys.exit()
        if not NAMELOOKUP.has_key(hostip):
            NAMELOOKUP[hostip] = hostname
        
        ADDRLOOKUP[hostid] = (hostip, hostport)
        IDLOOKUP[(hostip, hostport)] = hostid
        
def initSocket():
    global mysock
    mysock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 



if __name__ == "__main__":
    # initial logger
    global logger
    logging.basicConfig()
    logger = logging.getLogger("global")
    logger.setLevel(logging.INFO)
    
    parseHostfile()
    initSocket()
    print "command: (1) kill id (2) start id (3) start all"
    while(1):
        input = raw_input("input command: ")
        splitInput = input.rstrip("\r\n").split(" ")
        if(splitInput[0] == 'kill' and len(splitInput) == 2):
            mysock.sendto(input, ADDRLOOKUP[int(splitInput[1])])
        elif(splitInput[0] == 'start' and len(splitInput) == 2):
            if(splitInput[1] == 'all'):
                for addr in IDLOOKUP.keys():
                    mysock.sendto("start all", addr)
            else:
                mysock.sendto(input, ADDRLOOKUP[int(splitInput[1])])
        else:
            print 'unknown command'      
                 
