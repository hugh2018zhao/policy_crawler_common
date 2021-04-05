# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import datetime
import json

import scrapy


class RawItem(scrapy.Item):
    raw_key          = scrapy.Field()  # 按对方网站得到的原始主键
    url              = scrapy.Field()  # 页面url
    category         = scrapy.Field()  # 页面类别；目录页填写list，详情页填写detail
    source           = scrapy.Field()  # 网站代号
    html             = scrapy.Field()  # 页面原始html
    json_            = scrapy.Field()  # 请求的json
    extra            = scrapy.Field()  # 其他信息
    scraped_datetime = scrapy.Field()  # 抓取时间
    washed_data      = scrapy.Field()  # 清洗后的结果，以json存储


def itemify(raw_key, url, category="unknown", source="unknown",
            html="", json_="", extra="", washed_data=""):
    item = RawItem()
    item['raw_key']  = raw_key
    item['url']      = url
    item['category'] = category
    item['source']   = source
    item['html']     = html
    if isinstance(json_, str):
        item['json_'] = json_
    elif isinstance(json_, dict):
        item["json_"] = json.dumps(json_)

    if isinstance(extra, str):
        item['extra'] = extra
    elif isinstance(extra, dict):
        item["extra"] = json.dumps(extra)

    item['scraped_datetime'] = datetime.datetime.now()
    item['washed_data'] = washed_data
    return item


if __name__ == '__main__':
    item = RawItem()
