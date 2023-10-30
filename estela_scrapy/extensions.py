import json
import os
from datetime import datetime

import redis
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.exporters import PythonItemExporter
from twisted.internet import task

from estela_scrapy.utils import json_serializer, producer

from .utils import json_serializer, update_job

RUNNING_STATUS = "RUNNING"
COMPLETED_STATUS = "COMPLETED"


class BaseExtension:
    def __init__(self, stats, *args, **kwargs):
        self.stats = stats
        self.auth_token = os.getenv("ESTELA_AUTH_TOKEN")
        job = os.getenv("ESTELA_SPIDER_JOB")
        host = os.getenv("ESTELA_API_HOST")
        self.job_jid, spider_sid, project_pid = job.split(".")
        self.job_url = "{}/api/projects/{}/spiders/{}/jobs/{}".format(
            host, project_pid, spider_sid, self.job_jid
        )


class ItemStorageExtension(BaseExtension):
    def __init__(self, stats):
        super().__init__(stats)
        exporter_kwargs = {"binary": False}
        self.exporter = PythonItemExporter(**exporter_kwargs)

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler.stats)
        crawler.signals.connect(ext.item_scraped, signals.item_scraped)
        return ext

    def item_scraped(self, item, spider):
        item = self.exporter.export_item(item)
        data = {
            "jid": os.getenv("ESTELA_COLLECTION"),
            "payload": dict(item),
            "unique": os.getenv("ESTELA_UNIQUE_COLLECTION"),
        }
        producer.send("job_items", data)


class RedisStatsCollector(BaseExtension):
    def __init__(self, stats):
        super().__init__(stats)

        redis_url = os.getenv("REDIS_URL")
        if not redis_url:
            raise NotConfigured("REDIS_URL not found in the settings")
        self.redis_conn = redis.from_url(redis_url)

        self.stats_key = os.getenv("REDIS_STATS_KEY")
        self.interval = float(os.getenv("REDIS_STATS_INTERVAL"))

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler.stats)

        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)

        return ext

    def spider_opened(self, spider):
        update_job(self.job_url, self.auth_token, status=RUNNING_STATUS)
        self.task = task.LoopingCall(self.store_stats, spider)
        self.task.start(self.interval)

    def spider_closed(self, spider, reason):
        if self.task.running:
            self.task.stop()

        try:
            self.redis_conn.delete(self.stats_key)
        except Exception:
            pass

        stats = self.stats.get_stats()
        update_job(
            self.job_url,
            self.auth_token,
            status=COMPLETED_STATUS,
            lifespan=int(stats.get("elapsed_time_seconds", 0)),
            total_bytes=stats.get("downloader/response_bytes", 0),
            item_count=stats.get("item_scraped_count", 0),
            request_count=stats.get("downloader/request_count", 0),
            proxy_usage_data={
                "proxy_name": stats.get("downloader/proxy_name", ""),
                "bytes": stats.get("downloader/proxies/response_bytes", 0),
            },
        )

        parsed_stats = json.dumps(stats, default=json_serializer)
        data = {
            "jid": os.getenv("ESTELA_SPIDER_JOB"),
            "payload": json.loads(parsed_stats),
        }
        producer.send("job_stats", data)

    def store_stats(self, spider):
        stats = self.stats.get_stats()
        elapsed_time = int((datetime.now() - stats.get("start_time")).total_seconds())
        stats.update({"elapsed_time_seconds": elapsed_time})

        parsed_stats = json.dumps(stats, default=json_serializer)
        self.redis_conn.hmset(self.stats_key, json.loads(parsed_stats))
