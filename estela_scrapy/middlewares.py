import os

from scrapy.utils.request import request_fingerprint

from estela_scrapy.producer import producer
from estela_scrapy.utils import parse_time


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
