import os
import requests
import json

from estela_scrapy.utils import datetime_to_json
from scrapy import signals
from scrapy.exporters import PythonItemExporter
from estela_scrapy.producer import connect_kafka_producer, on_kafka_send_error

RUNNING_STATUS = "RUNNING"
COMPLETED_STATUS = "COMPLETED"
INCOMPLETE_STATUS = "INCOMPLETE"
FINISHED_REASON = "finished"


class ItemStorageExtension:
    def __init__(self, stats):
        self.stats = stats
        self.producer = connect_kafka_producer()
        exporter_kwargs = {"binary": False}
        self.exporter = PythonItemExporter(**exporter_kwargs)
        job = os.getenv("ESTELA_SPIDER_JOB")
        host = os.getenv("ESTELA_API_HOST")
        self.auth_token = os.getenv("ESTELA_AUTH_TOKEN")
        self.job_jid, spider_sid, project_pid = job.split(".")
        self.job_url = "{}/api/projects/{}/spiders/{}/jobs/{}".format(
            host, project_pid, spider_sid, self.job_jid
        )

    def spider_opened(self, spider):
        self.update_job_status(RUNNING_STATUS)

    def update_job_status(self, status):
        requests.patch(
            self.job_url,
            data={"status": status},
            headers={"Authorization": "Token {}".format(self.auth_token)},
        )

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler.stats)
        crawler.signals.connect(ext.item_scraped, signals.item_scraped)
        crawler.signals.connect(ext.spider_opened, signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signals.spider_closed)
        return ext

    def item_scraped(self, item):
        item = self.exporter.export_item(item)
        data = {
            "jid": os.getenv("ESTELA_COLLECTION"),
            "payload": dict(item),
            "unique": os.getenv("ESTELA_UNIQUE"),
        }
        self.producer.send("job_items", value=data).add_errback(on_kafka_send_error)

    def spider_closed(self, spider, reason):
        parser_stats = json.dumps(self.stats.get_stats(), default=datetime_to_json)
        data = {
            "jid": os.getenv("ESTELA_SPIDER_JOB"),
            "payload": json.loads(parser_stats),
        }
        self.update_job_status(
            COMPLETED_STATUS if reason == FINISHED_REASON else INCOMPLETE_STATUS
        )
        self.producer.send("job_logs", value=data).add_errback(on_kafka_send_error)
        self.producer.flush()