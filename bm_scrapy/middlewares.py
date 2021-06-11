import os

from bm_scrapy.utils import parse_time
from bm_scrapy.producer import connect_kafka_producer, on_kafka_send_error
from scrapy.utils.request import request_fingerprint


class StorageDownloaderMiddleware():
    def __init__(self):
        self.producer = connect_kafka_producer()

    def process_response(self, request, response, spider):
        data = {
            'jid': os.getenv('BM_SPIDER_JID'),
            'payload': {
                'url': response.url,
                'status': int(response.status),
                'method': request.method,
                'duration': int(request.meta.get('download_latency', 0) * 1000),
                'time': parse_time(),
                'response_size': len(response.body),
                'fingerprint': request_fingerprint(request),
            },
        }
        self.producer.send('job_requests', value=data).add_errback(on_kafka_send_error)
        # process parent request [!] missing
        return response
