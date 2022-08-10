from scrapy.statscollectors import StatsCollector

import logging

logger = logging.getLogger(__name__)


class EstelaStatsCollector(StatsCollector):
    def close_spider(self, spider, reason):
        super(EstelaStatsCollector, self).close_spider(spider, reason)
        logger.info("STATS")
        logger.info(self._stats)
