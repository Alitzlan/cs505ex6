import logging

# initial logger
logging.basicConfig()
logger = logging.getLogger("global")
logger.setLevel(logging.INFO)

def logdebug(id, msg):
    logger.debug("<{0}>:{1}".format(id, msg))
    
def loginfo(id, msg):
    logger.info("<{0}>:{1}".format(id, msg))
    
def logwarning(id, msg):
    logger.warning("<{0}>:{1}".format(id, msg))
    
def logerror(id, msg):
    logger.error("<{0}>:{1}".format(id, msg))