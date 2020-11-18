import pymongo
import scrapy
from scrapy import Request
from itemadapter import ItemAdapter

from pymongo import MongoClient
from pprint import pprint
from flask import Flask


class FlashbotSpider(scrapy.Spider):
    name = 'flashbot'
    allowed_domains = ['rss.jobsearch.monster.com']
    client = MongoClient()
    db = client.mydb
    my_collection = db.my_collection
    # Start the crawler at this URLs
    #start_urls = ['file://rss.jobsearch.monster.com/rssquery.ashx?q={query}']
    start_urls = ['http://rss.jobsearch.monster.com/rssquery.ashx?q={query}']

    thesaurus = ["machine learning", "machine", "learning", "big data", "big", "data"]

    LOG_LEVEL = "INFO"

    def parse(self, response):

        # We stat with this url
        url = self.start_urls[0]

        # Build and send a request for each word of the thesaurus
        for query in self.thesaurus:
            target = url.format(query=query)
            print("fetching the URL: %s" % target)
            if target.startswith("file://"):
                r = Request(target, callback=self.scrapit, dont_filter=True)
            else:
                r = Request(target, callback=self.scrapit)
            r.meta['query'] = query
            yield r

    def scrapit(self, response):
        query = response.meta["query"]

        # Base item with query used to this response
        print(query, response)

        # Scrap the data
        for doc in response.xpath("//item"):
            item = {"query": query}
            item["title"] = doc.xpath("title/text()").extract()
            item["description"] = doc.xpath("description/text()").extract()
            item["link"] = doc.xpath("link/text()").extract()
            item["pubDate"] = doc.xpath("pubDate/text()").extract()
            item["guid"] = doc.xpath("guid/text()").extract()

            guid = item ["guid"] [0]
            rest= self.my_collection.find({"guid":guid})
            if rest.count() ==0:
                self.my_collection.insert_one(item)
                print("document inserted", item ["title"])
                print("item scraped:", item["title"])
            yield item 
         #to download data: scrapy crawl flashbot -o toto.xml           

#flask:
app = Flask(__name__)
@app.route("/")
def root():
    return "Hello world"


