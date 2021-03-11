# -*- coding: utf-8 -*-
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import logging

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.utils.project import get_project_settings
from policy_crawler_common.mysql import Database

logger = logging.getLogger(__name__)
settings = get_project_settings()


class MysqlExportPipeline:

    @classmethod
    def from_crawler(cls, crawler):
        s = crawler.settings
        if not s.getbool('MySQL_EXPORT_ENABLED'):
            logger.warning("MySQL_EXPORT NotConfigured. ")
            raise NotConfigured
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        dbargs = spider.settings.get("DB_CONNECT").copy()
        table_name = spider.settings.get("TABLE_NAME")
        self.dbpool = Database(table_name, dbargs)

    def spider_closed(self, spider):
        self.dbpool.close()

    def process_item(self, item, spider):
        keys = item.keys()
        data = [item[k] for k in keys]
        self.dbpool.save(keys, [data])
        return item
