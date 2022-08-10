import requests
from scrapy.statscollectors import StatsCollector

import logging

logger = logging.getLogger(__name__)


class EstelaStatsCollector(StatsCollector):
    def close_spider(self, spider, reason):
        super(EstelaStatsCollector, self).close_spider(spider, reason)

        spider_stats = self.crawler.stats.get_stats()
        lifespan=spider_stats.get("elapsed_time_seconds", 0),
        total_bytes=spider_stats.get("downloader/response_bytes", 0),
        item_count=spider_stats.get("item_scraped_count", 0),
        request_count=spider_stats.get("downloader/request_count", 0),
        auth_token = os.getenv("ESTELA_AUTH_TOKEN")
        job = os.getenv("ESTELA_SPIDER_JOB")

        host = os.getenv("ESTELA_API_HOST")
        job_jid, spider_sid, project_pid = job.split(".")
        job_url = "{}/api/projects/{}/spiders/{}/jobs/{}".format(
            host, project_pid, spider_sid, self.job_jid
        )
        requests.patch(
            job_url,
            data={
                "lifespan": lifespan,
                "total_response_bytes": total_bytes,
                "item_count": item_count,
                "request_count": request_count,
            },
            headers={"Authorization": "Token {}".format(auth_token)},
        )

        #  parser_stats = json.dumps(spider_stats, default=datetime_to_json)
        #  data = {
            #  "jid": os.getenv("ESTELA_SPIDER_JOB"),
            #  "payload": json.loads(parser_stats),
        #  }
        #  self.producer.send("job_logs", value=data).add_errback(on_kafka_send_error)
        #  self.producer.flush()
