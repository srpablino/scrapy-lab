# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request


class IMDBSpider(scrapy.Spider):
    name = 'imdb'
    allowed_domains = ['www.imdb.com']
    start_urls = ['https://www.imdb.com/title/tt0096463/']
    actorsScrapped = []
    moviesScrapped = []

    def parse(self, response):
        idMovie = response.css('.add_to_checkins').xpath("@data-const").get()
        if idMovie in self.moviesScrapped:
            yield  None
            return

        titleSection = response.css('.title_wrapper').xpath('./h1')
        movieYear = titleSection.xpath('./span/a/text()').get()
        if movieYear is None:
            yield  None
            return
        if (int(movieYear) < 1980 or int(movieYear) > 1989):
            yield None
            return

        movieName = titleSection.xpath('./text()').get()
        actorList = response.css('.cast_list').xpath('./tr')[1::]
        nextScrap = []
        for c in actorList:
            actorURL = c.xpath('./td/a/@href').get()
            actorId = actorURL.split('/')[2]
            actorName = c.xpath('./td[1]/a/img/@alt').get()
            actorRole = c.xpath('./td[4]/a/text()').get()
            if actorURL is not None:
                nextScrap.append({"url": self.allowed_domains[0] + actorURL, "id": actorId})
            yield {"movie_id": idMovie,
                     "movie_name": movieName,
                     "movie_year": movieYear,
                     "actor_name": actorName,
                     "actor_id": actorId,
                     "role_name": actorRole
                     }
        self.moviesScrapped.append(idMovie)
        for a in nextScrap:
            if a['id'] not in self.actorsScrapped:
                self.actorsScrapped.append(a['id'])
                next_page = a['url']
                #yield response.follow(next_page, callback=self.parse_artist)
                yield Request("https://"+next_page, callback=self.parse_artist)

    def parse_artist(self, response):
        filmography = response.css('.filmo-category-section').xpath('./div')
        for film in filmography:
            next_page = self.allowed_domains[0]+film.xpath('./b/a/@href').get()
            yield Request("https://"+next_page,callback=self.parse)
