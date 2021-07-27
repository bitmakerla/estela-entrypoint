import os
import sys

from bm_scrapy import logger


def run_scrapy(argv, settings):
    from scrapy.cmdline import execute

    # an intermediate function might be needed for other commands [!] missing
    sys.argv = argv
    execute(settings=settings)


def run_code(args, commands_module=None):
    try:
        from bm_scrapy.settings import populate_settings

        # API data might be sent [!] missing
        settings = populate_settings()
        if commands_module:
            settings.set("COMMANDS_MODULE", commands_module, priority="cmdline")
    except:
        logger("Settings initialization failed")
        raise
    try:
        run_scrapy(args, settings)
    except Exception:
        logger("Job runtime exception")
        raise


def describe_project():
    """Describe scrapy project."""
    from bm_scrapy.env import setup_scrapy_conf

    setup_scrapy_conf()

    run_code(["scrapy", "describe_project"] + sys.argv[1:], "bm_scrapy.commands")


def setup_and_launch():
    try:
        from bm_scrapy.env import decode_job, get_args_and_env, setup_scrapy_conf

        job = decode_job()
        assert job, "JOB_INFO must be set"
        args, env = get_args_and_env(job)

        os.environ.update(env)
        setup_scrapy_conf()
        # init logger [!] missing
    except:
        print("Environment variables were not defined properly")
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
