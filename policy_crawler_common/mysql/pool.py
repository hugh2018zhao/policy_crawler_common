# -*- coding: utf-8 -*-

__author__ = 'BuGoNee'
__author__ = 'luyang911@gmail.com'

import sys
import logging
import pymysql as MySQLdb
from dbutils.pooled_db import PooledDB

logger = logging.getLogger(__name__)


class Database:
    """
    MySQL DB connect/close/select/insert.
    """
    # 连接池对象
    __pool = None

    def __init__(self, table_name, dbargs=dict()):
        self._table_name = table_name
        dbargs['cursorclass'] = MySQLdb.cursors.DictCursor
        try:
            self._conn = Database.__getConn(dbargs)
            self._cursor = self._conn.cursor()
        except Exception as e:
            logger.error(e)
            sys.exit()

    @staticmethod
    def __getConn(dbargs):
        """
        @summary: 静态方法，从连接池中取出连接
        @return MySQLdb.connection
        """
        if Database.__pool is None:
            __pool = PooledDB(creator=MySQLdb,
                              mincached=1,
                              maxcached=20,
                              **dbargs)
        return __pool.connection()

    def close(self):
        self._conn.commit()
        self._cursor.close()
        self._conn.close()

    def _execute(self, sql):
        cursor = self._cursor
        cursor.execute(sql)
        self._conn.commit()

    def _insert(self, sql, data):
        cursor = self._cursor
        cursor.executemany(sql, data)
        self._conn.commit()

    def _update(self, sql):
        self._execute(sql)

    def _select(self, sql):
        cursor = self._cursor
        cursor.execute(sql)
        data = cursor.fetchall()
        return data

    def save(self, keys, data):
        table_name = self._table_name
        fields = ', '.join(keys)
        qm = ','.join(['%s'] * len(keys))
        sql = r"""INSERT  {table_name} ({fields}) VALUES ({values}) ;"""
        sql = sql.format(table_name=table_name, fields=fields, values=qm)
        return self._insert(sql, data)

    def get(self, conditions="", limit=100000):
        table_name = self._table_name
        sql = r"""SELECT * FROM {table_name} """
        sql += ' WHERE ' + conditions if conditions else ""
        sql += ' LIMIT ' + str(limit) if limit else ""
        sql = sql.format(table_name=table_name) + " ; "
        return self._select(sql)

    def update(self, set_, conditions):
        table_name = self._table_name
        sql = r"""UPDATE {table_name} SET """.format(table_name=table_name)
        sql += ' ' + set_
        sql += ' WHERE ' + conditions
        sql += ' ; '
        return self._update(sql)


if __name__ == '__main__':
    DB_CONNECT = {
        'db': 'crawler',
        'user': 'root',
        'passwd': '',
        'host': 'localhost',
        'port': 3306,
        'charset': 'utf8mb4',
        'use_unicode': True,
    }
    table_name = "raw_data"
    db = Database(table_name, DB_CONNECT)
    keys = [
        "raw_key",
        "category",
        "source",
        "html",
        "json",
        "extra",
        "scraped_datetime",
    ]
    data = []

    import datetime
    for i in range(10):
        now = datetime.datetime.now()
        data.append([str(i), "cat", "c1", "html_test", "json_test", "extra_test", now])
    db.save(keys, data)
    dd = db.get()
    print(dd)
