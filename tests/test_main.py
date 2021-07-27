import os
import sys
from unittest import mock

from bm_scrapy.__main__ import run_scrapy
from bm_scrapy.__main__ import run_code
from bm_scrapy.__main__ import describe_project
from bm_scrapy.__main__ import setup_and_launch
from bm_scrapy.__main__ import main


JOB_INFO = '{"spider": "sample", "key": "6-6-6", "api_host": "http://bitmaker-api.com", "auth_token": ""}'


@mock.patch("scrapy.cmdline.execute")
def test_run_scrapy(mock_execute):
    run_scrapy(["scrapy", "crawl", "spider"], {"SETTING": "VALUE"})
    assert sys.argv == ["scrapy", "crawl", "spider"]
    assert mock_execute.called
    assert mock_execute.call_args == ({"settings": {"SETTING": "VALUE"}},)


@mock.patch("bm_scrapy.__main__.run_scrapy")
def test_run_code(mock_run_scrapy):
    run_code(["execution", "args"])
    assert mock_run_scrapy.called
    assert mock_run_scrapy.call_args[0][0] == ["execution", "args"]


@mock.patch("bm_scrapy.__main__.run_scrapy")
def test_run_code_commands_module(mock_run_scrapy):
    run_code(["execution", "args"], "commands_module")
    settings = mock_run_scrapy.call_args[0][1]
    assert settings["COMMANDS_MODULE"] == "commands_module"


@mock.patch.dict(os.environ, {"JOB_INFO": JOB_INFO})
@mock.patch("bm_scrapy.env.setup_scrapy_conf")
@mock.patch("bm_scrapy.__main__.run_code")
def test_setup_and_launch(mock_run_code, mock_setup_scrapy_conf):
    setup_and_launch()
    assert mock_run_code.called
    assert mock_setup_scrapy_conf.called
    expected_env = {
        "BM_SPIDER_JOB": "6-6-6",
        "BM_SPIDER_NAME": "sample",
        "BM_API_HOST": "http://bitmaker-api.com",
        "BM_AUTH_TOKEN": "",
    }
    expected_args = ["scrapy", "crawl", "sample"]
    run_code_args = mock_run_code.call_args[0]
    for key, value in expected_env.items():
        assert os.environ.get(key) == value
    assert run_code_args[0] == expected_args


@mock.patch("bm_scrapy.env.setup_scrapy_conf")
@mock.patch("bm_scrapy.__main__.run_code")
def test_describe_project(mock_run_code, mock_setup_scrapy_conf):
    sys.argv = [""]
    describe_project()
    assert mock_run_code.called
    assert mock_setup_scrapy_conf.called
    expected_args = ["scrapy", "describe_project"]
    run_args = mock_run_code.call_args[0]
    assert run_args[0] == expected_args
    assert run_args[1] == "bm_scrapy.commands"


@mock.patch("bm_scrapy.__main__.setup_and_launch")
def test_main(mock_setup_and_launch):
    main()
    assert mock_setup_and_launch.called
