import os
import json

from scrapy import signals
from scrapy.exporters import PythonItemExporter
from kafka import KafkaProducer


class ItemStorageExtension:
    def __init__(self):
        bootstrap_server = [
            '{}:{}'.format(os.getenv('KAFKA_ADVERTISED_HOST_NAME', '127.0.0.1'),
                           os.getenv('KAFKA_ADVERTISED_PORT', '9092'))
        ]
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_server,
            api_version=(0, 10),
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
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
        self.producer.send('spider-items', value=dict(item))
        self.producer.flush()

    def spider_closed(self, spider, reason):
        self.producer.send('spider-outcomes', value=reason)
        self.producer.flush()
