from scrapy.utils.request import request_fingerprint
from bm_scrapy.writer import pipe_writer


class StorageDownloaderMiddleware():
    def __init__(self):
        self.writer = pipe_writer

    def process_response(self, request, response, spider):
        self.writer.write_request(
            url=response.url,
            status=response.status,
            method=request.method,
            duration=request.meta.get('download_latency', 0) * 1000,
            rsize=len(response.body),
            fp=request_fingerprint(request),
        )
        # process parent request [!] missing
        return response
