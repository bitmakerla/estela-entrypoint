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

from estela_scrapy.utils import json_serializer, producer, update_job

RUNNING_STATUS = "RUNNING"
COMPLETED_STATUS = "COMPLETED"

# Performance optimization constants
TIME_CACHE_UPDATE_INTERVAL = 100  # Update time calculation every N items
TIMELINE_BUCKET_SIZE_SECONDS = 60  # Store timeline in minute buckets

# Efficiency thresholds (requests per item)
EFFICIENCY_EXCELLENT_THRESHOLD = 3
EFFICIENCY_GOOD_THRESHOLD = 4
EFFICIENCY_FAIR_THRESHOLD = 5
EFFICIENCY_POOR_THRESHOLD = 7

# Efficiency penalty factors (multipliers for success rate)
EFFICIENCY_EXCELLENT_FACTOR = 1.0    # No penalty (<= 3 requests/item)
EFFICIENCY_GOOD_FACTOR = 0.95        # 5% penalty (<= 4 requests/item)
EFFICIENCY_FAIR_FACTOR = 0.90        # 10% penalty (<= 5 requests/item)
EFFICIENCY_POOR_FACTOR = 0.80        # 20% penalty (<= 7 requests/item)
EFFICIENCY_VERY_POOR_FACTOR = 0.65   # 35% penalty (> 7 requests/item)

# Success rate calculation weights (configurable via environment variables)
DEFAULT_SUCCESS_RATE_GOAL_WEIGHT = 0.7
DEFAULT_SUCCESS_RATE_HTTP_WEIGHT = 0.3

# Memory limits
DEFAULT_MAX_DUPLICATE_TRACKING = 100_000
DEFAULT_TIMELINE_BUCKETS = 30


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

        # CORE: Always initialized
        redis_url = os.getenv("REDIS_URL")
        if not redis_url:
            raise NotConfigured("REDIS_URL not found in the settings")
        self.redis_conn = redis.from_url(redis_url)

        self.stats_key = os.getenv("REDIS_STATS_KEY")
        self.interval = float(os.getenv("REDIS_STATS_INTERVAL"))

        # ADVANCED METRICS: Feature flag (experimental)
        self.collect_advanced_metrics = (
            os.getenv("ESTELA_COLLECT_ADVANCED_METRICS", "false").lower() == "true"
        )

        if self.collect_advanced_metrics:
            self._init_advanced_metrics(schema, unique_field, max_buckets)
        else:
            # Set to None when disabled
            self.items_expected = None
            self.success_rate_goal_weight = None
            self.success_rate_http_weight = None
            self.schema = None
            self.unique_field = None
            self.max_buckets = None
            self.http_status_counter = None
            self.duplicate_items = None
            self.timeline = None

    def _init_advanced_metrics(self, schema, unique_field, max_buckets):
        """Initialize advanced metrics tracking (experimental feature)"""
        # ITEMS EXPECTED: Goal-based metrics
        items_expected_env = os.getenv("ITEMS_EXPECTED")
        self.items_expected = int(items_expected_env) if items_expected_env else None

        # SUCCESS RATE: Configurable weights
        self.success_rate_goal_weight = float(
            os.getenv("SUCCESS_RATE_GOAL_WEIGHT", DEFAULT_SUCCESS_RATE_GOAL_WEIGHT)
        )
        self.success_rate_http_weight = float(
            os.getenv("SUCCESS_RATE_HTTP_WEIGHT", DEFAULT_SUCCESS_RATE_HTTP_WEIGHT)
        )

        # Schema validation (works with any callable that raises exceptions)
        self.schema = schema
        self.valid_items = 0
        self.invalid_items = 0

        # Duplicate tracking with memory cap
        self.unique_field = unique_field
        self.max_duplicate_items = int(
            os.getenv("ESTELA_MAX_DUPLICATE_TRACKING", DEFAULT_MAX_DUPLICATE_TRACKING)
        )
        self.duplicate_items = set()

        # Timeline tracking (minute-based buckets for memory efficiency)
        self.max_buckets = max_buckets
        self.timeline = defaultdict(int)

        # HTTP status tracking
        self.http_status_counter = defaultdict(int)

        # Performance optimization: cache time calculations
        self.cached_elapsed_seconds = 0
        self.items_since_time_update = 0

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
        # ADVANCED: Initialize duplicate counter only if feature is enabled
        if self.collect_advanced_metrics:
            self.stats.set_value("advanced_metrics/items_duplicates", 0)

        update_job(self.job_url, self.auth_token, status=RUNNING_STATUS)
        self.task = task.LoopingCall(self.store_stats, spider)
        self.task.start(self.interval)

    def response_received(self, response, request, spider):
        # ADVANCED: HTTP status tracking
        if self.collect_advanced_metrics:
            self.http_status_counter[response.status] += 1

    def item_scraped(self, item, spider):
        # ADVANCED: Collect advanced metrics if enabled
        if self.collect_advanced_metrics:
            self._track_advanced_metrics(item, spider)

    def _track_advanced_metrics(self, item, spider):
        """Track advanced metrics for an item (experimental feature)"""
        # Schema validation (works with any callable: pydantic, custom validators, etc.)
        if self.schema:
            try:
                self.schema(**item)
                self.valid_items += 1
            except Exception:  # Generic exception - works with any validator
                self.invalid_items += 1

        # Timeline tracking with cached time calculation (performance optimization)
        self.items_since_time_update += 1
        if self.items_since_time_update >= TIME_CACHE_UPDATE_INTERVAL:
            # Update time every N items instead of every item
            start_time = self.stats.get_stats().get("start_time")
            if start_time:
                self.cached_elapsed_seconds = int(
                    (datetime.now() - start_time).total_seconds()
                )
            self.items_since_time_update = 0

        # Store in minute-based buckets (memory optimization: 60x fewer entries)
        bucket_minute = self.cached_elapsed_seconds // 60
        self.timeline[bucket_minute] += 1

        # Duplicate tracking with memory cap
        if self.unique_field and self.unique_field in item:
            # Only track if we haven't exceeded the limit
            if len(self.duplicate_items) < self.max_duplicate_items:
                value = item[self.unique_field]
                if value in self.duplicate_items:
                    self.stats.inc_value("advanced_metrics/items_duplicates")
                else:
                    self.duplicate_items.add(value)

    def _calculate_metrics(self, spider, status="running"):
        stats = self.stats.get_stats()

        # Calculate elapsed time using Scrapy's built-in start_time
        start_time = stats.get("start_time")
        if start_time is not None:
            now = datetime.now(
                timezone.utc) if start_time.tzinfo else datetime.now()
            elapsed_time = (now - start_time).total_seconds()
        else:
            # Fallback if start_time is not available (should not happen)
            elapsed_time = 0

        elapsed_minutes = elapsed_time / 60

        # CORE METRICS: Use Scrapy's built-in counters (no duplicates)
        items = self.stats.get_value("item_scraped_count", 0)
        pages = self.stats.get_value("response_received_count", 0)

        items_per_min = items / elapsed_minutes if elapsed_minutes > 0 else 0
        pages_per_min = pages / elapsed_minutes if elapsed_minutes > 0 else 0
        time_per_page = elapsed_time / pages if pages > 0 else 0

        total_requests = self.stats.get_value("downloader/request_count", 0)

        if self.collect_advanced_metrics and self.http_status_counter:
            status_200 = self.http_status_counter.get(200, 0)
        else:
            status_200 = self.stats.get_value("downloader/response_count", 0)

        http_success_rate = (status_200 / total_requests * 100) if total_requests > 0 else 0
        http_success_rate = min(100.0, http_success_rate)

        # Efficiency calculation based on requests per item
        requests_per_item_obtained = total_requests / items if items > 0 else float('inf')

        # Efficiency factor with penalties
        if requests_per_item_obtained <= EFFICIENCY_EXCELLENT_THRESHOLD:
            efficiency_factor = EFFICIENCY_EXCELLENT_FACTOR
        elif requests_per_item_obtained <= EFFICIENCY_GOOD_THRESHOLD:
            efficiency_factor = EFFICIENCY_GOOD_FACTOR
        elif requests_per_item_obtained <= EFFICIENCY_FAIR_THRESHOLD:
            efficiency_factor = EFFICIENCY_FAIR_FACTOR
        elif requests_per_item_obtained <= EFFICIENCY_POOR_THRESHOLD:
            efficiency_factor = EFFICIENCY_POOR_FACTOR
        else:
            efficiency_factor = EFFICIENCY_VERY_POOR_FACTOR

        # Calculate success rate based on whether items_expected is defined
        if self.items_expected:
            goal_achievement = (items / self.items_expected * 100) if self.items_expected > 0 else 0
            success_rate = (
                (goal_achievement * self.success_rate_goal_weight +
                 http_success_rate * self.success_rate_http_weight) * efficiency_factor
            )
            success_rate = min(100, max(0, success_rate))
        else:
            goal_achievement = None
            success_rate = http_success_rate * efficiency_factor
            success_rate = min(100, max(0, success_rate))

        peak_mem = self.stats.get_value("memusage/max", 0)

        # Base metrics (always included)
        metrics = {
            "spider_name": spider.name,
            "status": status,
            "items_per_minute": round(items_per_min, 2),
            "pages_per_minute": round(pages_per_min, 2),
            "time_per_page_seconds": round(time_per_page, 2),
            "success_rate": round(success_rate, 2),
            "http_success_rate": round(http_success_rate, 2),
            "requests_per_item": round(requests_per_item_obtained, 2) if items > 0 and requests_per_item_obtained != float('inf') else 0,
            "efficiency_factor": round(efficiency_factor, 2),
            "resources/peak_memory_bytes": peak_mem,
        }

        # ADVANCED METRICS: Only if feature flag is enabled (prefixed with advanced_metrics/)
        if self.collect_advanced_metrics:
            # Goal achievement metrics
            metrics["advanced_metrics/goal_achievement"] = round(goal_achievement or 0, 2)

            # Schema validation metrics
            total_checked = self.valid_items + self.invalid_items
            schema_coverage_percentage = (
                (self.valid_items / total_checked) * 100 if total_checked > 0 else 0
            )
            metrics["advanced_metrics/schema_coverage/percentage"] = round(schema_coverage_percentage, 2)
            metrics["advanced_metrics/schema_coverage/valid"] = self.valid_items
            metrics["advanced_metrics/schema_coverage/checked"] = total_checked

            # HTTP status tracking
            for status_code, count in self.http_status_counter.items():
                metrics[f"advanced_metrics/http_errors/{status_code}"] = count

            # Retry reasons
            retry_reasons = {
                k.replace("retry/reason_count/", ""): v
                for k, v in stats.items()
                if k.startswith("retry/reason_count/")
            }
            for reason, count in retry_reasons.items():
                metrics[f"advanced_metrics/retries/by_reason/{reason}"] = count

            # Timeline aggregation (minute-based buckets)
            interval_size = max(1, math.ceil(elapsed_minutes / self.max_buckets))
            metrics["advanced_metrics/timeline_interval_minutes"] = interval_size

            aggregated = defaultdict(int)
            # Timeline is now stored as minute buckets, aggregate into larger intervals
            for minute, count in self.timeline.items():
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

            for i, interval_data in enumerate(timeline_sorted):
                metrics[f"advanced_metrics/timeline/{i}/interval"] = interval_data["interval"]
                metrics[f"advanced_metrics/timeline/{i}/items"] = interval_data["items"]

        return metrics, elapsed_time

    def spider_closed(self, spider, reason):
        if self.task.running:
            self.task.stop()

        metrics, elapsed_time = self._calculate_metrics(spider, status=reason)

        stats = self.stats.get_stats()
        stats.update({"elapsed_time_seconds": int(elapsed_time)})
        stats.update(metrics)

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
        metrics, elapsed_time = self._calculate_metrics(
            spider, status="running")

        stats = self.stats.get_stats()
        stats.update({"elapsed_time_seconds": int(elapsed_time)})
        stats.update(metrics)

        parsed_stats = json.dumps(stats, default=json_serializer)
        self.redis_conn.hmset(self.stats_key, json.loads(parsed_stats))
