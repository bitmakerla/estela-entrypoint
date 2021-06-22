import os

from scrapy import signals
from scrapy.exporters import PythonItemExporter
from bm_scrapy.producer import connect_kafka_producer, on_kafka_send_error


class ItemStorageExtension:
    def __init__(self):
        self.producer = connect_kafka_producer()
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
        data = {
            'jid': os.getenv('BM_SPIDER_JID'),
            'payload': dict(item),
        }
        self.producer.send('job_items', value=data).add_errback(on_kafka_send_error)

    def spider_closed(self, spider, reason):
        data = {
            'jid': os.getenv('BM_SPIDER_JID'),
            'payload': {
                'finish_reason': str(reason),
            },
        }
        self.producer.send('job_logs', value=data).add_errback(on_kafka_send_error)
        self.producer.flush()
