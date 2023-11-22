import logging
import os
import re

from scrapy.exceptions import NotConfigured
from scrapy.utils.python import to_bytes
from scrapy.utils.request import request_fingerprint
from twisted.web import http

from estela_scrapy.utils import parse_time, producer

proxy_logger = logging.getLogger("proxy_mw")


def get_header_size(headers):
    size = 0
    for key, value in headers.items():
        if isinstance(value, (list, tuple)):
            for v in value:
                size += len(b": ") + len(key) + len(v)
    return size + len(b"\r\n") * (len(headers.keys()) - 1)


def get_status_size(response_status):
    return len(to_bytes(http.RESPONSES.get(response_status, b""))) + 15


class StorageDownloaderMiddleware:
    def process_response(self, request, response, spider):
        data = {
            "jid": os.getenv("ESTELA_SPIDER_JOB"),
            "payload": {
                "url": response.url,
                "status": int(response.status),
                "method": request.method,
                "duration": int(request.meta.get("download_latency", 0) * 1000),
                "time": parse_time(),
                "response_size": len(response.body),
                "fingerprint": request_fingerprint(request),
            },
        }
        producer.send("job_requests", data)
        return response


class EstelaProxyMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        estela_proxies_enabled = os.getenv("ESTELA_PROXIES_ENABLED", False)
        if not estela_proxies_enabled:
            raise NotConfigured
        return cls(crawler.stats, crawler.spider)

    def __init__(self, stats, spider):
        self.username = os.getenv("ESTELA_PROXY_USER", "")
        self.password = os.getenv("ESTELA_PROXY_PASS", "")
        self.port = os.getenv("ESTELA_PROXY_PORT", "")
        self.url = os.getenv("ESTELA_PROXY_URL", "")

        proxy_scheme = "https" if self.url.startswith("https://") else "http"
        host_wo_schema = re.sub(f"^{proxy_scheme}://", "", self.url)
        self.full_proxy_url = f"{proxy_scheme}://{self.username}:{self.password}@{host_wo_schema}:{self.port}"

        self.stats = stats
        self.stats.set_value(
            "downloader/proxy_name", os.getenv("ESTELA_PROXY_NAME"), spider=spider
        )

    def process_request(self, request, spider):
        if not request.meta.get("proxies_disabled"):
            proxy_logger.debug("Using proxies with request %s", request.url)
            request.meta["proxy"] = self.full_proxy_url
            self.stats.inc_value("downloader/proxies/count", spider=spider)

    def process_response(self, request, response, spider):
        if not request.meta.get("proxies_disabled"):
            reslen = (
                len(response.body)
                + get_header_size(response.headers)
                + get_status_size(response.status)
                + 4
            )
            self.stats.inc_value(
                "downloader/proxies/response_bytes", reslen, spider=spider
            )
        return response
