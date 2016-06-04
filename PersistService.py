from pymongo import MongoClient
import config

client = MongoClient(host=config.MONGO_HOST, port=int(config.MONGO_PORT), connect=False, maxPoolSize=2)
db = client['crawler']
pages = db.pages

def save(url, content, headers):
    page={}
    page['url']=url
    page['text']=content
    page['header']=headers
    pages.insert_one(page)