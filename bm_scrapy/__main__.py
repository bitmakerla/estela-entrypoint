import os
import sys


def run_scrapy(argv, settings):
    from scrapy.cmdline import execute
    # an intermediate function might be needed for other commands [!] missing
    sys.argv = argv
    execute(settings=settings)

    
def run_code(args, commands_module=None):
    try:
        # define and populate setting
        from bm_scrapy.settings import populate_settings
        # API shub data might be sent [!] missing
        settings = populate_settings()
        if commands_module:
            settings.set('COMMANDS_MODULE', commands_module, priority='cmdline')
    except:
        print("Settings initialization failed")
        raise
    try:
        # run spider
        run_scrapy(args, settings)
    except Exception:
        print("Job runtime exception")
        raise


def describe_project():
    # bm-describe-project command
    from bm_scrapy.env import setup_scrapy_conf
    setup_scrapy_conf()

    # custom scrapy command + args
    run_code(['scrapy','describe_project'] + sys.argv[1:], 'bm_scrapy.commands')


def setup_and_launch():
     # JOB_INFO='{"spider": "books", "key":"projectid/spiderid/jobid"}'
    try:
        from bm_scrapy.env import decode_job, get_args_and_env, setup_scrapy_conf
        # Read Job info from env
        job = decode_job()
        assert job, 'JOB_INFO must be set'
        args, env = get_args_and_env(job)
        os.environ.update(env)
        setup_scrapy_conf()
        #init logger [!] missing
    except:
        print("Environment variables were not defined properly")
        raise
    
    run_code(args)

    
def crawl():
    # bm-crawl command
    try:
        from bm_scrapy.writer import pipe_writer
        pipe_writer.open()
    except Exception:
        print("Error while opening the fifo pipe in the writer")
        return 1
    try:
        setup_and_launch()
    except:
        return 1
    return 0
