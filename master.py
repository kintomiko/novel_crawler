import redis
import logging, sys
import config
from pybloom import ScalableBloomFilter
sbf = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)

logger = logging.getLogger('Slave')
logger.setLevel(logging.INFO)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)

logger.addHandler(ch)

redis_conn = redis.StrictRedis(config.REDIS_HOST, config.REDIS_PORT)

while(True):
  url = redis_conn.blpop(config.RawQueue)[1]

  if not url in sbf:
    sbf.add(url)
    redis_conn.rpush(config.PendingCrawlingQueue, url)
