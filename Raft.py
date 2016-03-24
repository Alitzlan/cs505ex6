import json
    
class RaftState:
    Follower, Candidate, Leader = range(3)

class MessageType:
    RequestVote, Vote, Ping = range(3)

class MessageBody:
    type = MessageType.Ping
    term = 0
    id = -1
    
    def __init__(self, s=str()):
        for key, val in json.loads(s, encoding="ascii"):
            setattr(self, key, val)
            
    def __init__(self, type=MessageType.Ping, term=0, id=-1):
        self.type = type
        self.term = term
        self.id = id
    
    def toString(self):
        return json.dumps(self.__dict__, encoding="ascii")