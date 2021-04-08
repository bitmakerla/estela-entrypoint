import os
import json


def decode_job():
    job_data = os.getenv('JOB_INFO','')
    if job_data.startswith('{'):
        return json.loads(job_data)


def get_args_and_env(msg):
    args = ['scrapy', 'crawl', str(msg['spider'])]
    # consider API args and settings [!] missing
    env = {
        'BM_JOB': msg['key'],
        'BM_SPIDER': msg['spider']
    }
    return args, env


def setup_scrapy_conf():
    # scrapy.cfg is required by scrapy.utils.project.data_path
    if not os.path.exists('scrapy.cfg'):
        open('scrapy.cfg', 'w').close()
        
