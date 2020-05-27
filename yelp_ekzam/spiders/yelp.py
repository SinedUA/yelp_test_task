# -*- coding: utf-8 -*-
import scrapy
from yelp_ekzam.items import YelpEkzamItem
import re
import json

# command to run the spider: scrapy crawl yelp -a find=xxx -a near=yyy
# xxx = category
# yyy = location (if location consist of few words, print locations in quotes ("Los Angeles") or use underscore (Los_Angeles))

# BD_PW: 14061987


class YelpSpider(scrapy.Spider):
    name = 'yelp'
    allowed_domains = ['yelp.com']

    # also can use settings.py
    custom_settings = {
    #    'CRAWLERA_ENABLED': True,
    #    'DOWNLOAD_DELAY':3,
        'ROBOTSTXT_OBEY': False,
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36',
    }

    def __init__(self, find=None, near=None, **kwargs):

        super(YelpSpider, self).__init__(**kwargs)
        self.find = find
        self.near = near

    def start_requests(self):
        # go to link with filter options of appropriate location and category
        url = 'https://www.yelp.com/search?find_desc={}&find_loc={}&ns=1'.format(self.find, self.near.replace(' ', '+').replace('_', '+'))
        yield scrapy.Request(
            url=url,
            meta={
                'near': self.near,
                'find': self.find
            },
            callback=self.parse_loc
        )

    def parse_loc(self, response):
        # use subfilters (districts of the city)
        # to overcome the limit of 1000 results of search (yelp limits)
        data = json.loads(re.search('"place":(.*?)\,"category":{"moreFilters":', str(response.body)).group(1).replace('\\', ''))
        regions_urls = []
        for index in range(len(data["moreFilters"])):
            loc_filter = data["moreFilters"][index]["subfilters"]
            for loc in loc_filter:
                elements = [x.strip() for x in loc.split(':') if x.strip() != '']
                if len(elements) == 4:
                    url = response.url + '&l=p%3A{}%3A{}%3A{}%3A{}'.format(elements[0], elements[1], elements[2], elements[3])
                    regions_urls.append(url)
                if len(elements) == 3:
                    url = response.url + '&l=p%3A{}%3A{}%3A%3A{}'.format(elements[0], elements[1], elements[2])
                    regions_urls.append(url)

        for url in regions_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse
            )

    def parse(self, response):
        links = [x.strip() for x in response.xpath('//*[contains(@class, "link-color--inherit")]/@href').extract() if '/biz/' in x.strip()]
        for url in links:
            yield scrapy.Request(
                url='https://www.yelp.com' + url,
                callback=self.parse_item
            )

        next_page = response.xpath('//*[@rel="next"]/@href').extract_first(default='')
        if next_page:
            yield scrapy.Request(
                url=next_page,
                callback=self.parse
            )

    def parse_item(self, response):
        i = YelpEkzamItem()
        i['Title'] = response.xpath('//*[contains(@class, "heading--h1__373c0")]/text()').extract_first(default='')
        i['Address'] = ', '.join([x.strip() for x in response.xpath('//*[contains(@class, "lemon--address")]//text()').extract() if x.strip() != ''])
        i['Phone'] = ', '.join([x.strip() for x in response.xpath('//*[contains(text(), "Phone number")]/following-sibling::p/text()').extract() if x.strip() != ''])
        i['Web'] = ', '.join([x.strip() for x in response.xpath('//*[contains(@href, "website_link_type=website")]//text()').extract() if x.strip() != ''])

        schedule = []
        schedule_row = response.xpath('//*[@class="lemon--tbody__373c0__2T6Pl"]/tr')
        for row in schedule_row:
            day = row.xpath('.//text()').extract()
            if len(day) > 3 and 'now' in ', '.join(day):
                day.insert(2, '&')
            if len(day) > 2 and 'now' not in ', '.join(day):
                day.insert(2, '&')
                schedule.append(" ".join([x.strip() for x in day if 'now' not in x.strip()]))
            else:
                schedule.append(" ".join([x.strip() for x in day if 'now' not in x.strip()]))
        i['Schedule'] = ';\n'.join(schedule)

        try:
            about_1 = re.search('"specialtiesText":"(.*?)\","', str(response.body)).group(1).replace('\\n', '').replace('\\', '').replace('"', '').split('}}')[0].split('}')[0]
        except:
            about_1 = ''

        try:
            about_2 = re.search('"historyText":"(.*?)\\."', str(response.body)).group(1).replace('\\n', '').replace('\\', '').replace('"', '').split('}}')[0].split('}')[0]
        except:
            about_2 = ''
        about = '{}\n{}'.format(about_1, about_2)
        i['About'] = about
        if len(about) < 7:
            i['About'] = ''
        i['Image'] = response.xpath('//*[contains(@class, "photo-header-media__")]//@src').extract_first(default='')
        i['Reviews_number'] = response.xpath('//*[contains(@itemprop, "aggregateRating")]//*[@itemprop="reviewCount"]/text()').extract_first(default='')
        if i['Reviews_number'] == '':
            i['Reviews_number'] = 0

        yield i

    """   variant #2 - getting all feedback (text / number)"""

    #     reviews_number = len(re.findall('{"reviewRating"', str(response.body)))
    #     i['Reviews_number'] = reviews_number

    #     next_page = response.xpath('//*[@rel="next"]/@href').extract_first(default='')
    #     if len(next_page) > 5:
    #         yield scrapy.Request(
    #             url=next_page,
    #             meta={
    #                 'i': i,
    #                 'reviews_number': reviews_number
    #             },
    #             callback=self.parse_reviews_number
    #         )
    #     else:
    #         yield i

    # def parse_reviews_number(self, response):
    #     i = response.meta.get('i')
    #     reviews_number = response.meta.get('reviews_number') + len(re.findall('{"reviewRating"', str(response.body)))
    #     i['Reviews_number'] = reviews_number

    #     next_page = response.xpath('//*[@rel="next"]/@href').extract_first(default='')
    #     if next_page:
    #         yield scrapy.Request(
    #             url=next_page,
    #             meta={
    #                 'i': i,
    #                 'reviews_number': reviews_number
    #             },
    #             callback=self.parse_reviews_number
    #         )
    #     else:
    #         yield i