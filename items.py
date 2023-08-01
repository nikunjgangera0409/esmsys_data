# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class DistrictItem(scrapy.Item):
    id = scrapy.Field()
    d_name = scrapy.Field()

class TalukaItem(scrapy.Item):
    d_id = scrapy.Field()
    t_id = scrapy.Field()
    t_name = scrapy.Field()

class VillageItem(scrapy.Item):
    t_id = scrapy.Field()
    v_id = scrapy.Field()
    v_name = scrapy.Field()
