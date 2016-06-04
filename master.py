import redis
import logging, sys
import config
from pybloom import ScalableBloomFilter
sbf = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)

logger = Logger.getStdOutDebugLogger('master')

redis_conn = redis.StrictRedis(config.REDIS_HOST, config.REDIS_PORT)

while(True):
  url = redis_conn.blpop(config.RawQueue)[1]

  if (not url in sbf) and (url[0:url.find('/', 7)].find('2epub') != -1):
    sbf.add(url)
    redis_conn.rpush(config.PendingCrawlingQueue, url)
