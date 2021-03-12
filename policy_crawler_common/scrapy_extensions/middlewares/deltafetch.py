# -*- coding: utf-8 -*-
__author__ = 'BuGoNee'

import logging
import os

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.http import Request
from scrapy.item import BaseItem
from scrapy.utils.project import get_project_settings

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
    def __init__(self, key, db, table, stats=None):
        self.DELTAFETCH_KEY_NAME = key
        self.db = DeltaFetchDbWrapper(table, db)
        self.stats = stats
        logger.info("DELTAFETCH INIT. %s, %s, %s" % (self.DELTAFETCH_KEY_NAME,
                                                    self.db, self.stats))

    @classmethod
    def from_crawler(cls, crawler):
        s = crawler.settings
        if not s.getbool('DELTAFETCH_ENABLED'):
            logger.info("DELTAFETCH NotConfigured. ")
            raise NotConfigured
        key = s.get('DELTAFETCH_KEY_NAME')
        db = s.get('DELTAFETCH_DB_NAME')
        table = s.get('DELTAFETCH_TABLE_NAME')
        o = cls(key, db, table, crawler.stats)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_closed(self, spider):
        self.db.close()

    def spider_opened(self, spider):
        pass

    def process_spider_output(self, response, result, spider):
        pairs = dict()
        for r in result:
            if isinstance(r, Request):
                key = self._get_key(r)
                if key:
                    pairs[key] = r
                    continue
            elif isinstance(r, (BaseItem, dict)):
                key = self._get_key(response.request)
                if self.stats:
                    self.stats.inc_value('deltafetch/stored', spider=spider)
            yield r
        keys = pairs.keys()
        exists_keys = self.db.has_key(self.DELTAFETCH_KEY_NAME, keys) if keys else []
        exists_keys = [e.get(self.DELTAFETCH_KEY_NAME) for e in exists_keys]
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


class DeltaFetchDbWrapper(Database):
    def has_key(self, key_name, keys, limit=None):
        if not keys:
            logger.error("keys empty: %s" % keys)
            return []
        qm = ','.join(["'%s'"] * len(keys))
        statement = ' %s in (%s) ' % (key_name, qm)
        statement = statement % tuple(keys)
        logger.debug("db statement: %s" % statement)
        return self.get(statement, limit)

if __name__ == '__main__':
    pass
