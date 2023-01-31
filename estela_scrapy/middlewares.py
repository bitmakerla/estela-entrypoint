import os

from scrapy.utils.request import request_fingerprint

from estela_scrapy.producer import connect_kafka_producer, on_kafka_send_error
from estela_scrapy.utils import parse_time


class StorageDownloaderMiddleware:
    def __init__(self):
        self.producer = connect_kafka_producer()

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
        self.producer.send("job_requests", value=data).add_errback(on_kafka_send_error)
        # process parent request [!] missing
        return response
