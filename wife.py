import sys 
import socket
import time
import subprocess as sp
import logging
import main


if __name__ == "__main__":
    # initial logger
    main.initLogger()
    # initialization
    main.parseOpt()
    main.initSocket()
    extProc = None
    while(1):
        print 'waiting command'
        data, addr = main.mysock.recvfrom(512)
        print addr,":",data
        splitInput = data.rstrip("\r\n").split(" ")
        if(splitInput[0] == 'kill' and len(splitInput)==2 and extProc != None):
            # kill
            sp.Popen.terminate(extProc)
            extProc = None 
        elif(splitInput[0]== 'start' and len(splitInput)==2 and extProc==None):
            #start 
            extProc = sp.Popen(['python','main.py'])   
        else:
            print 'invalid command'        
        
    
    