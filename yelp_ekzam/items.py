# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class YelpEkzamItem(scrapy.Item):
    # define the fields for your item here like:
    Title = scrapy.Field()
    Address = scrapy.Field()
    Phone = scrapy.Field()
    Email = scrapy.Field()
    Web = scrapy.Field()
    Schedule = scrapy.Field()
    About = scrapy.Field()
    Image = scrapy.Field()
    Reviews_number = scrapy.Field()