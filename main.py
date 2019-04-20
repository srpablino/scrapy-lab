from scrapy import cmdline

#cmdline.execute("scrapy crawl nytimes -o nytimes.json".split())
cmdline.execute("scrapy crawl imdb -o imdb.json".split())