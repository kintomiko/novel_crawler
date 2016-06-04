import requests
import logging
import re
import traceback

import Logger
import URLService

logger = Logger.getStdOutDebugLogger('FileExtensionProcessor')

class SaveProcessor:
  def process(url, content, headers):
    try:
      logger.info(threading.current_thread().name + 'is working on url: '+url + ' and processing...')
      PersistService.save(url, content, headers)
    except:
      traceback.print_exc()
      logger.info('======Error')
    finally:
      logger.info('======Finished processing '+ url)

class UrlFilter:
  def __init__(self, pattern, processor): 
    self.processor = processor
    self.pattern = re.compile(pattern)

  def process(url, content, headers):
    if re.match(pattern, url):
      processor.process(url, content, headers)

patterns = ['.*\.txt$', '.*\.epub$', '.*\.mobi$']
filters = []
for pattern in patterns:
  filters.append(UrlFilter(pattern, SaveProcessor()))

def proc(url, content, headers):
  for f in filters:
    f.process(url, content, headers)
