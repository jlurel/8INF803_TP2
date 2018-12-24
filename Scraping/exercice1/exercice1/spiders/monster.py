# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

from exercice1.items import Monster

class MonsterSpider(CrawlSpider):
    name = 'monster'
    allowed_domains = ['d20pfsrd.com']
    # on parcourt toutes les pages de spell
    start_urls = ['http://www.d20pfsrd.com/bestiary/bestiary-alphabetical/']

    rules = {
        Rule(LinkExtractor(allow=r'\/bestiary\/bestiary-alphabetical\/bestiary-*'), follow=True),
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//a[contains(@href,"monster-listings")]',)),
             callback="parse_items")
    }

    def parse_items(self, response):
        item = Monster()
        item['name'] = response.xpath('//p[@class="title"]/text()[1]').extract()[0].strip()
        item['spells'] = []
        spells = response.xpath('//a[@class="spell"]/text()').extract()
        for spell in zip(spells):
            if spell not in item['spells']:
                item['spells'].append(spell)

        yield item
