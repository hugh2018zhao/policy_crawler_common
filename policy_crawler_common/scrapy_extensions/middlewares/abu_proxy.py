# -*- coding: utf-8 -*-
__author__ = 'BuGoNee'

import logging
import base64

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.utils.project import get_project_settings

logger = logging.getLogger(__name__)
settings = get_project_settings()


class AbuProxyMiddleware(object):
    """A middleware to enable http proxy to selected spiders only.
    """

    @classmethod
    def from_crawler(cls, crawler):
        s = crawler.settings
        logger.info("PROXY MIDDLEWARE fromcrawler")
        if not s.getbool('PROXY_ENABLED'):
            logger.info("PROXY NotConfigured. ")
            raise NotConfigured

        middleware = cls()
        crawler.signals.connect(middleware.spider_opened, signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signals.spider_closed)
        return middleware

    def spider_closed(self, spider):
        pass

    def spider_opened(self, spider):
        self.proxy_server = "http://http-dyn.abuyun.com:9020"
        user_pass = spider.settings.get("ABU_USER_PASS")
        self.auth = "Basic " + base64.urlsafe_b64encode(bytes(user_pass, "ascii")).decode("utf8")
        logger.info("PROXY MIDDLEWARE INIT. USERPASS: %s, AUTH: %s" % (user_pass, self.auth))

    def process_request(self, request, spider):
        if request.meta.get('dont_proxy') is True:
            logger.debug('dont_proxy <%s>:<True>' % request)
            return
        request.meta["proxy"] = self.proxy_server
        request.headers["Proxy-Authorization"] = self.auth

    def process_exception(self, request, exception, spider):
        pass


if __name__ == '__main__':
    px = AbuProxyMiddleware(settings={})
    px.spider_opened('aaa')
    pass
