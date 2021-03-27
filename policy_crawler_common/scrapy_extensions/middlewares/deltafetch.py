# -*- coding: utf-8 -*-
__author__ = 'BuGoNee'

import logging

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.http import Request
from scrapy.item import Item
from scrapy.utils.project import get_project_settings
from crawlab.db.mongo import get_col

logger = logging.getLogger(__name__)
settings = get_project_settings()


class DeltaFetchMiddleware(object):
    """
    This is a spider middleware to ignore requests to pages containing items
    seen in previous crawls of the same spider, thus producing a "delta crawl"
    containing only new items.

    This also speeds up the crawl, by reducing the number of requests that need
    to be crawled, and processed (typically, item requests are the most cpu
    intensive).
    """
    def __init__(self, key, stats=None):
        self.DELTAFETCH_KEY_NAME = key
        self.col = get_col()
        self.stats = stats
        logger.info("DELTAFETCH INIT. %s, %s, %s" % (self.DELTAFETCH_KEY_NAME, self.col, self.stats))

    @classmethod
    def from_crawler(cls, crawler):
        s = crawler.settings
        if not s.getbool('DELTAFETCH_ENABLED'):
            logger.info("DELTAFETCH NotConfigured. ")
            raise NotConfigured
        key = s.get('DELTAFETCH_KEY_NAME')
        o = cls(key, crawler.stats)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_closed(self, spider):
        pass

    def spider_opened(self, spider):
        self.spider_name = spider.name

    def process_spider_output(self, response, result, spider):
        pairs = dict()
        for r in result:
            if isinstance(r, (Item, dict)):
                key = self._get_key(response.request)
                if self.stats:
                    self.stats.inc_value('deltafetch/stored', spider=spider)
            elif isinstance(r, Request):
                key = self._get_key(r)
                if key:
                    pairs[key] = r
                    continue
            yield r

        keys = list(pairs.keys())
        exists_keys = []
        if keys:
            key_name = self.DELTAFETCH_KEY_NAME
            exists_keys = self._has_key(source=self.spider_name, key_name=key_name, data=keys)
        logger.debug("EXISTS KEYS IN DB: %s" % exists_keys)
        for key, r in pairs.items():
            if key in exists_keys:
                logger.debug("Ignoring already visited: %s,%s" % (key, r))
                if self.stats:
                    self.stats.inc_value('deltafetch/skipped', spider=spider)
                continue
            yield r

    def _get_key(self, request):
        key = request.meta.get('deltafetch_key', '')
        return str(key)

    def _has_key(self, source="", key_name="raw_key", data=[]):
        result = self.col.find({key_name: {"$in": data}, "source": source})
        return [r.get(key_name) for r in result]


if __name__ == '__main__':
    pass
