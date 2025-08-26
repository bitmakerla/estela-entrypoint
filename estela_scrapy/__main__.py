import logging
import os
import sys

from estela_scrapy.env import decode_job, get_args_and_env
from estela_scrapy.log import init_logging
from estela_scrapy.settings import populate_settings


def run_scrapy(argv, settings):
    from scrapy.cmdline import execute

    sys.argv = argv
    execute(settings=settings)


def run_code(args, log_handler=None, commands_module=None):
    try:
        settings = populate_settings()
        if commands_module:
            settings.set("COMMANDS_MODULE", commands_module, priority="cmdline")
        if log_handler is not None:
            log_handler.setLevel(settings["LOG_LEVEL"])
    except Exception:
        logging.exception("Settings initialization failed.")
        raise
    try:
        run_scrapy(args, settings)
    except Exception as ex:
        logging.exception(f"Job runtime exception: {str(ex)}")
        raise


def describe_project():
    from estela_scrapy.env import setup_scrapy_conf

    setup_scrapy_conf()

    run_code(
        ["scrapy", "describe_project"] + sys.argv[1:],
        commands_module="estela_scrapy.commands",
    )


def report_deploy():
    from estela_scrapy.env import setup_scrapy_conf

    setup_scrapy_conf()

    run_code(
        ["scrapy", "report_deploy"] + sys.argv[1:],
        commands_module="estela_scrapy.commands",
    )


def setup_and_launch():
    try:
        job = decode_job()
        assert job, "JOB_INFO must be set"
        args, env = get_args_and_env(job)

        from estela_scrapy.env import setup_scrapy_conf

        os.environ.update(env)
        setup_scrapy_conf()

        loghdlr = init_logging()
    except Exception:
        logging.exception("Environment variables were not defined properly.")
        raise

    run_code(args, loghdlr)


def main():
    from estela_scrapy.utils import producer

    try:
        if producer.get_connection():
            logging.debug("Successful connection to the queue platform.")
        else:
            raise Exception("Could not connect to the queue platform.")
        setup_and_launch()
        code = 0
    except SystemExit as ex:
        code = ex.code
    except:
        code = 1
    finally:
        producer.flush()
        producer.close()

    return code


if __name__ == "__main__":
    sys.exit(main())
