import os
import mock

from bm_scrapy.env import decode_job
from bm_scrapy.env import get_args_and_env
from bm_scrapy.env import  setup_scrapy_conf


@mock.patch.dict(os.environ, {'JOB_INFO': '{"key": "value"}'})
def test_decode_job():
    assert decode_job() == {"key":"value"}

    
def test_decode_job_no_env_variable():
    assert decode_job() == None


def test_get_args_and_env():
    msg = {"spider": "demo", "key":"1/2/3"}
    result = get_args_and_env(msg)
    assert len(result) == 2
    assert result[0] == ['scrapy', 'crawl', 'demo']
    assert result[1] == {'BM_JOB': '1/2/3', 'BM_SPIDER': 'demo'}


@mock.patch('builtins.open')
def test_setup_scrapy_conf(mock_open):
    setup_scrapy_conf()
    assert mock_open.called
    
