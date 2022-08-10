import os
import signal
import sys
import logging
from scrapy.crawler import CrawlerProcess
from scrapy.utils.ossignal import install_shutdown_handlers, signal_names
from scrapy.utils.misc import create_instance, load_object

logger = logging.getLogger(__name__)


class MyCP(CrawlerProcess):
    def _signal_shutdown(self, signum, _):
        from twisted.internet import reactor
        install_shutdown_handlers(lambda _: pass)  # do not allow killing the job
        signame = signal_names[signum]
        logger.info("Received %(signame)s, shutting down gracefully. Send again to force ",
                    {'signame': signame})
        reactor.callFromThread(self._graceful_stop_reactor)

    def _print_whatevs(self):
        logger.info("PLEASE WORK HERE")
        logger.info(f"CRAWLERS {list(self.crawlers)}")
        logger.info("STATS", list(self.crawlers)[0].stats.get_stats())
        #  d.addBoth(self._stop_reactor)

    def _graceful_stop_reactor(self):
        d = self.stop()
        d.addBoth(self._print_whatevs)
        return d

    #  def _signal_shutdown(self, signum, _):
        #  from twisted.internet import reactor
#
        #  install_shutdown_handlers(self._signal_kill)
        #  signame = signal_names[signum]
        #  logger.info(
            #  "Received %(signame)s, shutting down gracefully. Send again to force ",
            #  {"signame": signame},
        #  )
        #  reactor.callFromThread(self._graceful_stop_reactor)

def print_data(crawler_process):
    logger.info("MADE IT 2")
    logger.info(f"CRAWLERS {list(crawler_process.crawlers)}")
    logger.info("STATS", list(crawler_process.crawlers)[0].stats.get_stats())


def print_whatevs(crawler_process):
    d = crawler_process
    #  logger.info("MADE IT HAHAHA")
    #  d = crawler_process.stop()
    #  d.addBoth(print_data, crawler_process)
    #  return d
    #  logger.info("MADE IT 2")
    #  print_data(crawler_process)


def run_scrapy(argv, settings, describe):
    if describe:
        from scrapy.cmdline import execute

        #  an intermediate function might be needed for other commands [!] missing
        sys.argv = argv
        execute(settings=settings)
    else:
        from scrapy import spiderloader
        from scrapy.crawler import Crawler, CrawlerProcess
        from twisted.internet import reactor

        spider_loader = spiderloader.SpiderLoader.from_settings(settings)
        print(f"SCRAPING SPIDER {argv[2]}")
        spider_class = spider_loader.load(argv[2])
        print(f"SPIDER CLASS {spider_class}")
        spider = spider_class()
        print(f"SPIDER {spider}")

        #  crawler_process = CrawlerProcess(settings)
        #  crawler = crawler_process.create_crawler(spider_class)
        #  crawler.signals.connect(print_whatevs, signal.SIGUSR1)
        #  crawler.crawl()
        #  d = crawler_process.join()
        #  if not d.called:
        #  d.addBoth(crawler_process._stop_reactor)

        #  crawler_process.crawl(argv[2])
        #  signal.signal(signal.SIGUSR1, lambda signum, frame: reactor.callFromThread(print_whatevs, crawler_process))
        #  crawler_process.start(stop_after_crawl=False)

        crawler_process = MyCP(settings)
        crawler_process.crawl(argv[2])
        crawler_process.start()


def run_code(args, commands_module=None, describe=False):
    try:
        from estela_scrapy.settings import populate_settings

        # API data might be sent [!] missing
        settings = populate_settings()
        if commands_module:
            settings.set("COMMANDS_MODULE", commands_module, priority="cmdline")
    except Exception:
        logging.exception("Settings initialization failed")
        raise
    try:
        run_scrapy(args, settings, describe)
    except Exception:
        logging.exception("Job runtime exception")
        raise


def describe_project():
    """Describe scrapy project."""
    from estela_scrapy.env import setup_scrapy_conf

    setup_scrapy_conf()

    run_code(
        ["scrapy", "describe_project"] + sys.argv[1:],
        "estela_scrapy.commands",
        describe=True,
    )


def setup_and_launch():
    try:
        from estela_scrapy.env import decode_job, get_args_and_env, setup_scrapy_conf

        job = decode_job()
        assert job, "JOB_INFO must be set"
        args, env = get_args_and_env(job)

        os.environ.update(env)
        setup_scrapy_conf()
        # init logger [!] missing
    except:
        logging.exception("Environment variables were not defined properly")
        raise

    run_code(args)


def main():
    """Start the crawling process."""
    try:
        setup_and_launch()
    except SystemExit as ex:
        return ex.code
    except:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
