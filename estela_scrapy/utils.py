from datetime import datetime

from estela_queue_adapter import get_producer_interface


def parse_time(date=None):
    if date is None:
        date = datetime.now()
    parsed_time = date.strftime("%d/%m/%Y %H:%M:%S.%f")[:-3]
    return parsed_time


def datetime_to_json(o):
    if isinstance(o, datetime):
        return o.__str__()
    raise TypeError("Type {} not serializable".format(type(o)))


def to_standar_str(text, encoding="utf-8", errors="strict"):
    if isinstance(text, str):
        return text
    if not isinstance(text, bytes):
        raise TypeError("Unable to standardize {} type".format(type(text).__name__))
    return text.decode(encoding, errors)


producer = get_producer_interface()
