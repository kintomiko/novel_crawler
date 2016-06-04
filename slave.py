import re
import requests
import config
import sys
import threading
from multiprocessing.pool import ThreadPool

import Logger
import Filter
import URLService

maxconnections = 4
tp = ThreadPool(maxconnections)
pool_sema = threading.BoundedSemaphore(value=maxconnections)

PATTERN_URL = re.compile('href=\"((https?:\/\/)?([\da-zA-Z\.-]+)\.([a-zA-Z\.]{2,6})([\/\w\.-]*)\/?\??[\w=]*)\"')

logger = Logger.getStdOutDebugLogger('slave')

def result_handler(result):
  pool_sema.release()

while(True):
  logger.info('======Waiting new url')
  url = URLService.getPendingProcessUrl()
  pool_sema.acquire()
  tp.apply_async(func=loopProcess, args=(url,), callback=result_handler)


def loopProcess(url):
  r = requests.get(url)
  Filter.proc(url, r.text, r.headers)
# push all url into raw queue
  mats = re.findall(PATTERN_URL, r.text)
  if mats is not None:
    for mat in mats:
      link = mat[0]
      if not link.startswith('http://'):
        link = url[:url.find('/', 8)] + '/' + link
      URLService.pushRawUrl(link)