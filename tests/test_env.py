import os
from unittest import mock

from bm_scrapy.env import decode_job
from bm_scrapy.env import get_api_args
from bm_scrapy.env import get_args_and_env
from bm_scrapy.env import setup_scrapy_conf


@mock.patch.dict(os.environ, {"JOB_INFO": '{"key": "value"}'})
def test_decode_job():
    assert decode_job() == {"key": "value"}


def test_decode_job_no_env_variable():
    assert decode_job() is None


def test_get_api_args():
    assert get_api_args({}) == []
    assert get_api_args({"one": "two"}) == ["-a", "one=two"]


def test_get_args_and_env():
    msg = {
        "spider": "demo",
        "key": "1-2-3",
        "api_host": "http://estela-api.com",
        "auth_token": "",
        "args": {"arg1": "val1", "arg2": "val2"},
    }
    result = get_args_and_env(msg)
    assert len(result) == 2
    assert result[0] == [
        "scrapy",
        "crawl",
        "demo",
        "-a",
        "arg1=val1",
        "-a",
        "arg2=val2",
    ]
    assert result[1] == {
        "BM_SPIDER_JOB": "1-2-3",
        "BM_SPIDER_NAME": "demo",
        "BM_API_HOST": "http://estela-api.com",
        "BM_AUTH_TOKEN": "",
    }


@mock.patch("builtins.open")
def test_setup_scrapy_conf(mock_open):
    setup_scrapy_conf()
    assert mock_open.called
