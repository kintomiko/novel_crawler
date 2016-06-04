import logging
import sys

def getStdOutDebugLogger(name):
	logger = logging.getLogger(name)
	logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler(sys.stdout)
	ch.setLevel(logging.DEBUG)

	logger.addHandler(ch)
	return logger