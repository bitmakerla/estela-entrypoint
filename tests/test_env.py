import os
from unittest import mock

from estela_scrapy.env import (
    decode_job,
    get_api_args,
    get_args_and_env,
    setup_scrapy_conf,
)


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
        "collection": "sj-1-2",
        "unique": "True",
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
        "ESTELA_SPIDER_JOB": "1-2-3",
        "ESTELA_SPIDER_NAME": "demo",
        "ESTELA_API_HOST": "http://estela-api.com",
        "ESTELA_AUTH_TOKEN": "",
        "ESTELA_COLLECTION": "sj-1-2",
        "ESTELA_UNIQUE_COLLECTION": "True",
    }


@mock.patch("builtins.open")
def test_setup_scrapy_conf(mock_open):
    setup_scrapy_conf()
    assert mock_open.called
