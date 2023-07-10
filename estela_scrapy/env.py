import json
import os


def decode_job():
    job_data = os.getenv("JOB_INFO", "")
    if job_data.startswith("{"):
        return json.loads(job_data)


def get_api_args(args_dict):
    if not args_dict:
        return []
    args = []
    for key, value in dict(args_dict).items():
        args += ["-a", "{}={}".format(key, value)]
    return args


def get_args_and_env(msg):
    args = ["scrapy", "crawl", str(msg["spider"])]
    args += get_api_args(msg.get("args", {}))
    env = {
        "ESTELA_SPIDER_JOB": msg["key"],
        "ESTELA_SPIDER_NAME": msg["spider"],
        "ESTELA_API_HOST": msg["api_host"],
        "ESTELA_AUTH_TOKEN": msg["auth_token"],
        "ESTELA_COLLECTION": msg["collection"],
        "ESTELA_UNIQUE_COLLECTION": msg["unique"],
    }
    return args, env


def setup_scrapy_conf():
    # scrapy.cfg is required by scrapy.utils.project.data_path
    if not os.path.exists("scrapy.cfg"):
        open("scrapy.cfg", "w").close()
