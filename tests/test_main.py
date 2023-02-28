import json
import os
import sys
from unittest import mock

from estela_scrapy.__main__ import (
    describe_project,
    main,
    run_code,
    run_scrapy,
    setup_and_launch,
)

job_info_dict = {
    "key": "6-6-6",
    "spider": "sample",
    "auth_token": "",
    "api_host": "http://estela-api.com",
    "collection": "sj-1-2",
    "unique": "True",
}
JOB_ENV = {"JOB_INFO": json.dumps(job_info_dict)}


@mock.patch("scrapy.cmdline.execute")
def test_run_scrapy(mock_execute):
    run_scrapy(["scrapy", "crawl", "spider"], {"SETTING": "VALUE"})
    assert sys.argv == ["scrapy", "crawl", "spider"]
    assert mock_execute.called
    assert mock_execute.call_args == ({"settings": {"SETTING": "VALUE"}},)


@mock.patch("estela_scrapy.__main__.run_scrapy")
def test_run_code(mock_run_scrapy):
    run_code(["execution", "args"])
    assert mock_run_scrapy.called
    assert mock_run_scrapy.call_args[0][0] == ["execution", "args"]


@mock.patch("estela_scrapy.__main__.run_scrapy")
def test_run_code_commands_module(mock_run_scrapy):
    run_code(["execution", "args"], commands_module="commands_module")
    settings = mock_run_scrapy.call_args[0][1]
    assert settings["COMMANDS_MODULE"] == "commands_module"


@mock.patch.dict(os.environ, JOB_ENV)
@mock.patch("estela_scrapy.env.setup_scrapy_conf")
@mock.patch("estela_scrapy.__main__.run_code")
def test_setup_and_launch(mock_run_code, mock_setup_scrapy_conf):
    setup_and_launch()
    assert mock_run_code.called
    expected_env = {
        "ESTELA_SPIDER_JOB": "6-6-6",
        "ESTELA_SPIDER_NAME": "sample",
        "ESTELA_API_HOST": "http://estela-api.com",
        "ESTELA_AUTH_TOKEN": "",
        "ESTELA_COLLECTION": "sj-1-2",
        "ESTELA_UNIQUE_COLLECTION": "True",
    }
    expected_args = ["scrapy", "crawl", "sample"]
    run_code_args = mock_run_code.call_args[0]
    for key, value in expected_env.items():
        assert os.environ.get(key) == value
    assert run_code_args[0] == expected_args
    assert mock_setup_scrapy_conf.called


@mock.patch("estela_scrapy.env.setup_scrapy_conf")
@mock.patch("estela_scrapy.__main__.run_code")
def test_describe_project(mock_run_code, mock_setup_scrapy_conf):
    sys.argv = [""]
    describe_project()
    assert mock_run_code.called
    assert mock_setup_scrapy_conf.called
    expected_args = ["scrapy", "describe_project"]
    run_args, run_kwargs = mock_run_code.call_args
    assert run_args[0] == expected_args
    assert run_kwargs["commands_module"] == "estela_scrapy.commands"


@mock.patch("estela_scrapy.utils.producer.get_connection", return_value=True)
@mock.patch("estela_scrapy.__main__.setup_and_launch")
@mock.patch("estela_scrapy.utils.producer.flush")
@mock.patch("estela_scrapy.utils.producer.close")
def test_main(mock_get_conn, mock_close, mock_flush, mock_setup_and_launch):
    exit_code = main()
    assert mock_get_conn.called
    assert mock_setup_and_launch.called
    assert mock_flush.called
    assert mock_close.called
    assert exit_code == 0
