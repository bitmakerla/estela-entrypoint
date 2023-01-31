import json
import os
import sys
from datetime import datetime, timedelta

import redis
import requests
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.exporters import PythonItemExporter
from twisted.internet import task

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

    def item_scraped(self, item, spider):
        item = self.exporter.export_item(item)
        spider.crawler.stats.inc_value("database_size_sys", sys.getsizeof(item))
        spider.crawler.stats.inc_value(
            "database_size_json", len(json.dumps(item, default=str))
        )
        data = {
            "jid": os.getenv("ESTELA_COLLECTION"),
            "payload": dict(item),
            "unique": os.getenv("ESTELA_UNIQUE_COLLECTION"),
        }
        self.producer.send("job_items", value=data).add_errback(on_kafka_send_error)

    def spider_closed(self, spider, reason):
        spider_stats = self.stats.get_stats()
        self.update_job(
            status=COMPLETED_STATUS if reason == FINISHED_REASON else INCOMPLETE_STATUS,
            lifespan=int(spider_stats.get("elapsed_time_seconds", 0)),
            total_bytes=spider_stats.get("downloader/response_bytes", 0),
            item_count=spider_stats.get("item_scraped_count", 0),
            request_count=spider_stats.get("downloader/request_count", 0),
        )

        parser_stats = json.dumps(spider_stats, default=str)
        data = {
            "jid": os.getenv("ESTELA_SPIDER_JOB"),
            "payload": json.loads(parser_stats),
        }
        self.producer.send("job_stats", value=data).add_errback(on_kafka_send_error)
        self.producer.flush()


class RedisStatsCollector(object):
    def __init__(self, redis_url, stats_key, interval):
        self.redis_conn = redis.from_url(redis_url)
        self.stats_key = stats_key
        self.interval = interval

    @classmethod
    def from_crawler(cls, crawler):
        redis_url = os.getenv("REDIS_URL")
        stats_key = os.getenv("REDIS_STATS_KEY")
        interval = os.getenv("REDIS_STATS_INTERVAL")
        print("INTERVAL: ", interval)
        interval = float(interval)

        if not redis_url:
            raise NotConfigured("REDIS_URL not found in the settings")

        ext = cls(redis_url, stats_key, interval)

        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)

        return ext

    def spider_opened(self, spider):
        self.task = task.LoopingCall(self.store_stats, spider)
        self.task.start(self.interval)

    def spider_closed(self, spider, reason):
        if self.task.running:
            self.task.stop()
        try:
            self.redis_conn.delete(self.stats_key)
        except Exception:
            pass

    def store_stats(self, spider):
        stats = spider.crawler.stats.get_stats()
        elapsed_time = int((datetime.now() - stats.get("start_time")).total_seconds())
        database_size_sys = stats.get("database_size_sys", 0)
        database_size_json = stats.get("database_size_json", 0)
        stats.update(
            {
                "elapsed_time": elapsed_time,
                "database_size_sys": database_size_sys,
                "database_size_json": database_size_json,
            }
        )

        self.redis_conn.hmset(self.stats_key, stats)
