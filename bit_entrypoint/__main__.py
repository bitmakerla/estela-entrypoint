import os
import sys
import json

from scrapy.cmdline import execute

#setting module
from scrapy.settings import Settings
from scrapy.utils.project import get_project_settings
try:
    from scrapy.utils.deprecate import update_classpath
except ImportError:
    update_classpath = lambda x: x
    

#env module
def decode_job():
    job_data = os.getenv('JOB_INFO','')
    if job_data.startswith('{'):
        return json.loads(job_data)


#env module
def get_args_and_env(msg):
    args = ['scrapy', 'crawl', str(msg['spider'])]
    env = {
        'S_JOB': msg['key'],
        'S_SPIDER': msg['spider']
    }
    return args, env


#env module
def setup_scrapy_conf():
    # scrapy.cfg is required by scrapy.utils.project.data_path
    if not os.path.exists('scrapy.cfg'):
        open('scrapy.cfg', 'w').close()


#settings module        
def update_deprecated_classpaths(settings):
    # This method updates settings with dicts as values if they're deprecated
    for setting_key in settings.attributes.keys():
        setting_value = settings[setting_key]
        if hasattr(setting_value, 'copy_to_dict'):
            setting_value = setting_value.copy_to_dict()
        if not isinstance(setting_value, dict):
            continue
        for path in setting_value.keys():
            updated_path = update_classpath(path)
            if updated_path != path:
                order = settings[setting_key].pop(path)
                settings[setting_key][updated_path] = order
                
                
#settings module
def load_default_settings(settings):
    # Load the default APP settings e.g. HubstorageDownloaderMiddleware
    downloader_middlewares = {}
    spider_middlewares = {}
    extensions = {}
    settings.get('DOWNLOADER_MIDDLEWARES_BASE').update(downloader_middlewares)
    settings.get('EXTENSIONS_BASE').update(extensions)
    settings.get('SPIDER_MIDDLEWARES_BASE').update(spider_middlewares)
    # memory_limit [!] missing
    # set other default settings with max priority
    settings.setdict({
        'LOG_LEVEL': 'INFO'
    }, priority='cmdline')

    
#settings module
def populate_settings():
    assert 'scrapy.conf' not in sys.modules, "Scrapy settings already loaded"
    settings = get_project_settings().copy()
    update_deprecated_classpaths(settings)
    # consider use special class if there're problems with AWS and encoding [!] missing
    # consider API <shub> settings [!] missing -> https://shub.readthedocs.io/en/stable/custom-images-contract.html#shub-settings
    load_default_settings(settings)
    # load and merge API settings according to priority [job > spider > organization > project]
    # afeter merging, somre enforcement might be done [!] missing
    return settings


def run_spider(argv, settings):
    # an intermediate function might be needed for other commands [!] missing
    sys.argv = argv
    execute(settings=settings)

    
def run_code(args):
    try:
        # define and populate setting
        settings = populate_settings()
    except:
        print("Settings initialization failed")
        raise
    try:
        # run spider
        run_spider(args, settings)
    except Exception:
        print("Job runtime exception")
        raise

    
def crawl():
    # JOB_INFO: {"spider": "spidername", "key":"projectid/spiderid/jobid"}
    # set FIFO pipe [!] missing
    try:
        # Read Job info from env
        job = decode_job()
        assert job, 'JOB_INFO must be set'
        args, env = get_args_and_env(job)
        os.environ.update(env)
        setup_scrapy_conf()
    except:
        print("Environment variables were not defined properly")
        raise

    #init logger [!] missing
    run_code(args)
