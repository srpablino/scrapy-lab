# -*- coding: utf-8 -*-
import os
import uuid
from ssl import create_default_context

import certifi
import scrapy
from scrapy import Request
from elasticsearch import Elasticsearch
from scrapy.exceptions import CloseSpider

ELASTIC_API_URL_HOST = os.environ['ELASTIC_API_URL_HOST']
ELASTIC_API_URL_PORT = os.environ['ELASTIC_API_URL_PORT']
ELASTIC_API_USERNAME = os.environ['ELASTIC_API_USERNAME']
ELASTIC_API_PASSWORD = os.environ['ELASTIC_API_PASSWORD']

es=Elasticsearch(host=ELASTIC_API_URL_HOST,
                 port=ELASTIC_API_URL_PORT,
                 scheme='https',
                 http_auth=(ELASTIC_API_USERNAME,ELASTIC_API_PASSWORD)
                 )

class IMDBCloudSpider(scrapy.Spider):
    name = 'imdbCloud'
    allowed_domains = ['www.imdb.com']
    start_urls = ['https://www.imdb.com/title/tt0096463/fullcredits/']
    actorsScrapped = []
    moviesScrapped = []
    documentscount = 0

    def parse(self, response):
        type = response.css('head').xpath('./meta[@property="og:type"]/@content').get().split('.')[1]
        if not type == "movie":
            yield None
            return
        titleSection = response.css('.subpage_title_block')
        if titleSection is None:
            yield None
            return
        idMovie = titleSection.css(".parent").xpath("./h3/a/@href").get().split('/')[2]
        if idMovie in self.moviesScrapped:
            yield None
            return
        movieYear = \
        titleSection.css('.nobr').xpath('./text()').get().strip().replace(')', '(').split('(')[1].split(' ')[0]
        if movieYear is None:
            yield None
            return
        if (int(movieYear) < 1980 or int(movieYear) > 1989):
            yield None
            return

        movieName = titleSection.xpath('./div/h3/a/text()').get()
        actorList = response.css('.cast_list').xpath('./tr')[1::]
        nextScrap = []
        for c in actorList:
            if self.documentscount >= 5000:
                yield None
                raise CloseSpider('Number of documents reached')
            if (len(c.xpath('./td').getall()) < 3):
                continue;
            actorURL = c.xpath('./td/a/@href').get()
            actorId = actorURL.split('/')[2]
            actorName = c.xpath('./td[@class="primary_photo"]//a/img/@alt').get()
            actorRole = c.xpath('./td[@class="character"]/text()').get().strip().replace("\n", "")
            if actorRole == '':
                actorRole = c.xpath('./td[@class="character"]/a/text()').get()

            if actorURL is not None:
                nextScrap.append({"url": self.allowed_domains[0] + actorURL, "id": actorId})
            es.index(index="imdb",
                     doc_type="movies",
                     #id=uuid.uuid4(),
                     id=idMovie+'-'+actorId,
                     body={ "movie_id": idMovie,
                            "movie_name": movieName,
                            "movie_year": movieYear,
                            "actor_name": actorName,
                            "actor_id": actorId,
                            "role_name": actorRole
                            })
            self.documentscount = self.documentscount + 1
        self.moviesScrapped.append(idMovie)
        for a in nextScrap:
            if a['id'] not in self.actorsScrapped:
                self.actorsScrapped.append(a['id'])
                next_page = "https://" + a['url']
                yield Request(next_page, callback=self.parse_artist)

    def parse_artist(self, response):
        filmography = response.css('.filmo-category-section').xpath('./div')
        for film in filmography:
            next_page = self.allowed_domains[0] + film.xpath('./b/a/@href').get().split("?ref")[0] + 'fullcredits/'
            yield Request("https://" + next_page, callback=self.parse)