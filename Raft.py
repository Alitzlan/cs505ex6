import json
import socket

FOLLOWER_TIMEOUT = 1
CANDIDATE_TIMEOUT = 1
LEADER_TIMEOUT = 1

class MessageType:
    RequestVote, Vote, Ping = range(3)

class MessageBody:
    type = MessageType.Ping
    id = -1
    
    def __init__(self, s=str()):
        for key, val in json.loads(s, encoding="ascii"):
            setattr(self, key, val)
    
    def toString(self):
        return json.dumps(self.__dict__, encoding="ascii")
    
class RaftState:
    Follower, Candidate, Leader = range(3)
    
def followerLoop(mysock):
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
            pass
        except socket.error, msg:
            if msg[0] == 10054 and sys.platform == "win32":
                # ignore connection reset because UDP is connection-less
                pass
            else:
                logger.error(msg)
                sys.exit()
                
def candidateLoop(mysock):
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
                
def leaderLoop(mysock):
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
        except socket.timeout, msg:
            pass
        except socket.error, msg:
            if msg[0] == 10054 and sys.platform == "win32":
                # ignore connection reset because UDP is connection-less
                pass
            else:
                logger.error(msg)
                sys.exit()