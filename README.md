# Common库

本库旨在抽象出政策类网站爬虫系统中的通用组件，包括proxy/ua_rotate/RawItem等爬虫中必备的组件


# 如何安装
```
    1. git clone git@git.bilibili.co:phoenix/manga_crawler_common.git
    2. cd manga_crawler_common
    3. pip install .  # 注意不能少了这个点
```


# 目录情况简介

./mysql/pool.py

./scrapy_extensions/items/items.py  # 该文件中包含一个RawItem对象，是个高度抽象的数据结构，可以包含任意类型的raw data存入该结构中

./scrapy_extensions/middlewares/proxy.py  # 阿布云代理中间件，会将通过的request对象挂上代理

./scrapy_extensions/middlewares/ua_rotate.py  # 自动轮换user agent，可支持MOBILE类型和PC类型的UA

./scrapy_extensions/pipelines/mysql_export.py  # 将item写入mysql表中


# TODO
    1. 各组件的详细介绍
    2. 单元测试
    3. logger组件
    4. mongo export

