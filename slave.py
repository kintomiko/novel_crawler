import re
import requests
import redis
import config
import logging
import sys
import threading
import traceback
from pymongo import MongoClient
from multiprocessing.pool import ThreadPool

maxconnections = 4
tp = ThreadPool(maxconnections)
pool_sema = threading.BoundedSemaphore(value=maxconnections)

PATTERN_URL = re.compile('href=\"((https?:\/\/)?([\da-zA-Z\.-]+)\.([a-zA-Z\.]{2,6})([\/\w\.-]*)\/?\??[\w=]*)\"')
logger = logging.getLogger('Slave')
logger.setLevel(logging.INFO)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)

logger.addHandler(ch)

pool = redis.ConnectionPool(max_connections=2)
redis_conn = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, connection_pool=pool)
client = MongoClient(host=config.MONGO_HOST, port=int(config.MONGO_PORT), connect=False, maxPoolSize=2)
db = client['crawler']
pages = db.pages

def process_url(url):
  try:
    logger.info(threading.current_thread().name + 'is working on url: '+url + ' and processing...')
    r = requests.get(url)
    page={}
    page['url']=url
    page['text']=r.text
    pages.insert_one(page)

    mats = re.findall(PATTERN_URL, r.text)
    if mats is not None:
      for mat in mats:
        redis_conn.rpush(config.RawQueue, mat[0])
  except:
    traceback.print_exc()
    logger.info('======Error')
  finally:
    logger.info('======Finished processing '+ url)	 

def result_handler(result):
  pool_sema.release()

while(True):
  logger.info('======Waiting new url')
  url = redis_conn.blpop(config.PendingCrawlingQueue)[1]
  pool_sema.acquire()
  tp.apply_async(func=process_url, args=(url,), callback=result_handler)