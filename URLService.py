import redis
import config

pool = redis.ConnectionPool()
redis_conn = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, connection_pool=pool)

def getPendingProcessUrl():
	return redis_conn.blpop(config.PendingCrawlingQueue)[1]

def pushRawUrl(url):
	redis_conn.rpush(config.RawQueue, url)
# pipe = redis_conn.pipeline()
# pipe.execute()
# pipe.rpush(config.RawQueue, link)