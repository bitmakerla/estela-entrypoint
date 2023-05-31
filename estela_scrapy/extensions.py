import json
import os
from datetime import timedelta

import requests
from scrapy import signals
from scrapy.exporters import PythonItemExporter

from estela_scrapy.utils import json_serializer, producer

RUNNING_STATUS = "RUNNING"
COMPLETED_STATUS = "COMPLETED"


class ItemStorageExtension:
    def __init__(self, stats):
        self.stats = stats
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
        self.update_job(status=RUNNING_STATUS)

    def update_job(
        self,
        status,
        lifespan=timedelta(seconds=0),
        total_bytes=0,
        item_count=0,
        request_count=0,
    ):
        requests.patch(
            self.job_url,
            data={
                "status": status,
                "lifespan": lifespan,
                "total_response_bytes": total_bytes,
                "item_count": item_count,
                "request_count": request_count,
            },
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
            "unique": os.getenv("ESTELA_UNIQUE_COLLECTION"),
        }
        producer.send("job_items", data)

    def spider_closed(self, spider, reason):
        spider_stats = self.stats.get_stats()
        self.update_job(
            status=COMPLETED_STATUS,
            lifespan=spider_stats.get("elapsed_time_seconds", 0),
            total_bytes=spider_stats.get("downloader/response_bytes", 0),
            item_count=spider_stats.get("item_scraped_count", 0),
            request_count=spider_stats.get("downloader/request_count", 0),
        )

        parser_stats = json.dumps(spider_stats, default=json_serializer)
        data = {
            "jid": os.getenv("ESTELA_SPIDER_JOB"),
            "payload": json.loads(parser_stats),
        }
        producer.send("job_stats", data)
