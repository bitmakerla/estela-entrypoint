import os
from json import dumps
from scrapy import signals
from scrapy.exporters import PythonItemExporter

from bm_scrapy.writer import pipe_writer
from kafka import KafkaProducer


def connect_kafka_producer():
    _producer = None
    bootstrap_server = [
        '{}:{}'.format(os.getenv('KAFKA_ADVERTISED_HOST_NAME', '127.0.0.1'),
                       os.getenv('KAFKA_ADVERTISED_PORT', '9092'))
    ]
    _producer = KafkaProducer(bootstrap_servers=bootstrap_server, api_version=(0, 10),
                              value_serializer=lambda x: dumps(x).encode('utf-8'))
    return _producer


producer = connect_kafka_producer()


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
        producer.send('spider-items', value=dict(item))
        producer.flush()
        self.writer.write_item(item)

    def spider_closed(self, spider, reason):
        self.writer.write_fin(reason)
