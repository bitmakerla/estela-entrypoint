from scrapy import signals
from scrapy.exporters import PythonItemExporter

from bm_scrapy.writer import pipe_writer


class ItemStorageExtension:
    def __init__(self):
        self.writer = pipe_writer
        exporter_kwargs = {'binary': False}
        self.exporter = PythonItemExporter(**exporter_kwargs)

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        crawler.signals.connect(ext.item_scraped, signals.item_scraped)
        crawler.signals.connect(ext.spider_closed, signals.spider_closed)
        return ext

    def item_scraped(self, item):
        item = self.exporter.export_item(item)
        self.writer.write_item(item)

    def spider_closed(self, spider, reason):
        self.writer.write_fin(reason)
