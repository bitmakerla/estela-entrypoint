import json
import os
import math
from datetime import datetime, timezone
from collections import defaultdict

import redis
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.exporters import PythonItemExporter
from twisted.internet import task
from pydantic import ValidationError

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
        self.exporter = PythonItemExporter(**exporter_kwargs, dont_fail=True)

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
    def __init__(self, stats, schema=None, unique_field=None, max_buckets=30):
        super().__init__(stats)

        redis_url = os.getenv("REDIS_URL")
        if not redis_url:
            raise NotConfigured("REDIS_URL not found in the settings")
        self.redis_conn = redis.from_url(redis_url)

        self.stats_key = os.getenv("REDIS_STATS_KEY")
        self.interval = float(os.getenv("REDIS_STATS_INTERVAL"))

        self.schema = schema
        self.unique_field = unique_field
        self.max_buckets = max_buckets

        self.http_status_counter = defaultdict(int)
        self.duplicate_items = set()
        self.valid_items = 0
        self.invalid_items = 0
        self.field_coverage = defaultdict(lambda: {"complete": 0, "empty": 0})
        self.timeline = defaultdict(int)
        self.start_time = None

    @classmethod
    def from_crawler(cls, crawler):
        schema = getattr(crawler.spidercls, "schema", None)
        unique_field = getattr(crawler.spidercls, "unique_field", None)
        max_buckets = crawler.settings.getint("METRICS_TIMELINE_BUCKETS", 30)
        ext = cls(
            crawler.stats,
            schema=schema,
            unique_field=unique_field,
            max_buckets=max_buckets
        )

        crawler.signals.connect(
            ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(
            ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.response_received,
                                signal=signals.response_received)

        return ext

    def spider_opened(self, spider):
        self.start_time = datetime.now()

        self.stats.set_value("custom/items_scraped", 0)
        self.stats.set_value("custom/pages_processed", 0)
        self.stats.set_value("custom/items_duplicates", 0)

        update_job(self.job_url, self.auth_token, status=RUNNING_STATUS)
        self.task = task.LoopingCall(self.store_stats, spider)
        self.task.start(self.interval)

    def response_received(self, response, request, spider):
        self.stats.inc_value("custom/pages_processed")
        self.http_status_counter[response.status] += 1

    def item_scraped(self, item, spider):
        self.stats.inc_value("custom/items_scraped")

        if self.schema:
            try:
                self.schema(**item)
                self.valid_items += 1
            except ValidationError:
                self.invalid_items += 1

        for field, value in item.items():
            if value is None or value == "" or value == []:
                self.field_coverage[field]["empty"] += 1
            else:
                self.field_coverage[field]["complete"] += 1

        if self.start_time:
            elapsed_seconds = int(
                (datetime.now() - self.start_time).total_seconds())
            self.timeline[elapsed_seconds] += 1

        if self.unique_field and self.unique_field in item:
            value = item[self.unique_field]
            if value in self.duplicate_items:
                self.stats.inc_value("custom/items_duplicates")
            else:
                self.duplicate_items.add(value)

    def spider_closed(self, spider, reason):
        if self.task.running:
            self.task.stop()

        stats = self.stats.get_stats()

        elapsed_time = (datetime.now() - self.start_time).total_seconds()
        elapsed_minutes = elapsed_time / 60

        items = self.stats.get_value("custom/items_scraped", 0)
        pages = self.stats.get_value("custom/pages_processed", 0)

        items_per_min = items / elapsed_minutes if elapsed_minutes > 0 else 0
        pages_per_min = pages / elapsed_minutes if elapsed_minutes > 0 else 0
        time_per_page = elapsed_time / pages if pages > 0 else 0

        successful_requests = self.stats.get_value(
            "downloader/response_count", 0)
        total_requests = self.stats.get_value("downloader/request_count", 0)
        retries_total = self.stats.get_value("retry/count", 0)

        success_rate = (
            (successful_requests / (total_requests - retries_total) * 100)
            if (total_requests - retries_total) > 0 else 0
        )

        total_checked = self.valid_items + self.invalid_items
        schema_coverage_percentage = (
            (self.valid_items / total_checked) * 100 if total_checked > 0 else 0
        )

        timeouts = self.stats.get_value(
            "downloader/exception_type_count/twisted.internet.error.TimeoutError", 0
        )

        retry_reasons = {
            k.replace("retry/reason_count/", ""): v
            for k, v in stats.items()
            if k.startswith("retry/reason_count/")
        }

        peak_mem = self.stats.get_value("memusage/max", 0)
        total_bytes = self.stats.get_value("downloader/response_bytes", 0)

        interval_size = max(1, math.ceil(elapsed_minutes / self.max_buckets))

        aggregated = defaultdict(int)
        for sec, count in self.timeline.items():
            minute = sec // 60
            bucket_start = (minute // interval_size) * interval_size
            bucket_end = bucket_start + interval_size
            label = f"{bucket_start}-{bucket_end}m"
            aggregated[label] += count

        timeline_sorted = [
            {"interval": k, "items": v}
            for k, v in sorted(
                aggregated.items(),
                key=lambda x: int(x[0].split("-")[0])
            )
        ]

        final_metrics = {
            "spider_name": spider.name,
            "status": reason,
            "elapsed_time_seconds": round(elapsed_time, 2),
            "items_scraped": items,
            "pages_processed": pages,
            "items_per_minute": round(items_per_min, 2),
            "pages_per_minute": round(pages_per_min, 2),
            "time_per_page_seconds": round(time_per_page, 2),
            "success_rate": round(success_rate, 2),
            "schema_coverage": {
                "percentage": round(schema_coverage_percentage, 2),
                "valid": self.valid_items,
                "checked": total_checked,
                "fields": dict(self.field_coverage),
            },
            "http_errors": dict(self.http_status_counter),
            "duplicates": self.stats.get_value("custom/items_duplicates", 0),
            "timeouts": timeouts,
            "retries": {
                "total": retries_total,
                "by_reason": retry_reasons,
            },
            "resources": {
                "peak_memory_bytes": peak_mem,
                "downloaded_bytes": total_bytes,
            },
            "timeline": timeline_sorted,
            "timeline_interval_minutes": interval_size,
        }

        stats.update({"elapsed_time_seconds": int(elapsed_time)})
        stats.update({"metrics": final_metrics})

        try:
            self.redis_conn.delete(self.stats_key)
        except Exception:
            pass

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
        start_time = stats.get("start_time")
    
        elapsed_time = None
        if start_time is not None:
            now = datetime.now(timezone.utc) if start_time.tzinfo else datetime.now()
            elapsed_time = int((now - start_time).total_seconds())
            stats.update({"elapsed_time_seconds": elapsed_time})
    
        if not self.start_time:
            self.start_time = stats.get("start_time", datetime.now())

        if elapsed_time is None:
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
        elapsed_minutes = elapsed_time / 60

        items = self.stats.get_value("custom/items_scraped", 0)
        pages = self.stats.get_value("custom/pages_processed", 0)

        items_per_min = items / elapsed_minutes if elapsed_minutes > 0 else 0
        pages_per_min = pages / elapsed_minutes if elapsed_minutes > 0 else 0
        time_per_page = elapsed_time / pages if pages > 0 else 0

        successful_requests = self.stats.get_value(
            "downloader/response_count", 0)
        total_requests = self.stats.get_value("downloader/request_count", 0)
        retries_total = self.stats.get_value("retry/count", 0)

        success_rate = (
            (successful_requests / (total_requests - retries_total) * 100)
            if (total_requests - retries_total) > 0 else 0
        )

        total_checked = self.valid_items + self.invalid_items
        schema_coverage_percentage = (
            (self.valid_items / total_checked) * 100 if total_checked > 0 else 0
        )

        timeouts = self.stats.get_value(
            "downloader/exception_type_count/twisted.internet.error.TimeoutError", 0
        )

        retry_reasons = {
            k.replace("retry/reason_count/", ""): v
            for k, v in stats.items()
            if k.startswith("retry/reason_count/")
        }

        peak_mem = self.stats.get_value("memusage/max", 0)
        total_bytes = self.stats.get_value("downloader/response_bytes", 0)

        interval_size = max(1, math.ceil(elapsed_minutes / self.max_buckets))

        aggregated = defaultdict(int)
        for sec, count in self.timeline.items():
            minute = sec // 60
            bucket_start = (minute // interval_size) * interval_size
            bucket_end = bucket_start + interval_size
            label = f"{bucket_start}-{bucket_end}m"
            aggregated[label] += count

        timeline_sorted = [
            {"interval": k, "items": v}
            for k, v in sorted(
                aggregated.items(),
                key=lambda x: int(x[0].split("-")[0])
            )
        ]

        metrics = {
            "spider_name": spider.name,
            "status": "running",
            "elapsed_time_seconds": round(elapsed_time, 2),
            "items_scraped": items,
            "pages_processed": pages,
            "items_per_minute": round(items_per_min, 2),
            "pages_per_minute": round(pages_per_min, 2),
            "time_per_page_seconds": round(time_per_page, 2),
            "success_rate": round(success_rate, 2),
            "schema_coverage": {
                "percentage": round(schema_coverage_percentage, 2),
                "valid": self.valid_items,
                "checked": total_checked,
                "fields": dict(self.field_coverage),
            },
            "http_errors": dict(self.http_status_counter),
            "duplicates": self.stats.get_value("custom/items_duplicates", 0),
            "timeouts": timeouts,
            "retries": {
                "total": retries_total,
                "by_reason": retry_reasons,
            },
            "resources": {
                "peak_memory_bytes": peak_mem,
                "downloaded_bytes": total_bytes,
            },
            "timeline": timeline_sorted,
            "timeline_interval_minutes": interval_size,
        }

        stats.update({"elapsed_time_seconds": int(elapsed_time)})
        stats.update({"metrics": metrics})

        parsed_stats = json.dumps(stats, default=json_serializer)
        self.redis_conn.hmset(self.stats_key, json.loads(parsed_stats))
