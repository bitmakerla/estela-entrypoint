from datetime import datetime

import pytest
import requests

from estela_scrapy import utils
from estela_scrapy.utils import parse_time, update_job


def test_parse_time():
    great_date = datetime(1985, 8, 14, 7, 10, 1, 123456)
    assert parse_time(great_date) == "14/08/1985 07:10:01.123"


def mock_patch(monkeypatch, status_codes):
    calls = []

    def fake_patch(url, **kwargs):
        calls.append(kwargs)
        response = requests.Response()
        response.status_code = status_codes[len(calls) - 1]
        return response

    monkeypatch.setattr(utils.requests, "patch", fake_patch)
    monkeypatch.setattr(utils.time, "sleep", lambda seconds: None)
    return calls


def test_update_job_retries_until_success(monkeypatch):
    calls = mock_patch(monkeypatch, [502, 200])
    update_job("http://api/jobs/1", "token", status="RUNNING")
    assert len(calls) == 2
    assert calls[0]["timeout"] == utils.UPDATE_JOB_TIMEOUT


def test_update_job_raises_after_last_attempt(monkeypatch):
    calls = mock_patch(monkeypatch, [502, 502, 502])
    with pytest.raises(requests.HTTPError):
        update_job("http://api/jobs/1", "token", status="RUNNING")
    assert len(calls) == utils.UPDATE_JOB_ATTEMPTS
