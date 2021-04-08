import os
import sys
import json
import pytest
import mock
from scrapy.settings import Settings

from bm_scrapy.__main__ import run_scrapy
from bm_scrapy.__main__ import run_code
from bm_scrapy.__main__ import describe_project
from bm_scrapy.__main__ import crawl


@mock.patch('scrapy.cmdline.execute')
def test_run_scrapy(mock_execute):
    run_scrapy(['scrapy', 'crawl', 'spider'], {'SETTING': 'VALUE'})
    assert sys.argv == ['scrapy', 'crawl', 'spider']
    assert mock_execute.called
    assert mock_execute.call_args == ({'settings': {'SETTING': 'VALUE'}},)
    

@mock.patch('bm_scrapy.__main__.run_scrapy')
def test_run_code(mock_run_scrapy):
    run_code(['execution','args'])
    assert mock_run_scrapy.called
    assert mock_run_scrapy.call_args[0][0] == ['execution','args']


@mock.patch('bm_scrapy.__main__.run_scrapy')
def test_run_code_commands_module(mock_run_scrapy):
    run_code(['execution','args'], 'commands_module')
    settings = mock_run_scrapy.call_args[0][1]
    assert isinstance(settings, Settings)
    assert settings['COMMANDS_MODULE'] == 'commands_module'


# New tests must be included with API args and setting support
JOB_SAMPLE = {'spider': 'sample', 'key':'6/6/6'}


@mock.patch.dict(os.environ, {'JOB_INFO':json.dumps(JOB_SAMPLE)})
@mock.patch('bm_scrapy.env.setup_scrapy_conf')
@mock.patch('bm_scrapy.__main__.run_code')
def test_crawl(mock_run_code, mock_setup_scrapy_conf):
    crawl()
    expected_env = {
        'BM_JOB': '6/6/6',
        'BM_SPIDER': 'sample'
    }
    expected_args = ['scrapy', 'crawl', 'sample']
    run_code_args = mock_run_code.call_args[0]
    assert mock_run_code.called
    assert mock_setup_scrapy_conf.called
    for key, value in expected_env.items():
        assert os.environ.get(key) == value
    assert run_code_args[0] == expected_args
    
