from datetime import datetime

from estela_scrapy.utils import parse_time


def test_parse_time():
    great_date = datetime(1985, 8, 14, 7, 10, 1, 123456)
    assert parse_time(great_date) == "14/08/1985 07:10:01.123"
