from datetime import datetime


def parse_time(date=None):
    if date is None:
        date = datetime.now()
    parsed_time = date.strftime("%d/%m/%Y %H:%M:%S.%f")[:-3]
    return parsed_time


def datetime_to_json(o):
    if isinstance(o, datetime):
        return o.__str__()
    raise TypeError("Type %s not serializable" % type(obj))
