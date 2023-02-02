from datetime import date, datetime


def parse_time(date=None):
    if date is None:
        date = datetime.now()
    parsed_time = date.strftime("%d/%m/%Y %H:%M:%S.%f")[:-3]
    return parsed_time


def json_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if hasattr(obj, "__str__"):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def to_standard_str(text, encoding="utf-8", errors="strict"):
    if isinstance(text, str):
        return text
    if not isinstance(text, bytes):
        raise TypeError("Unable to standardize {} type".format(type(text).__name__))
    return text.decode(encoding, errors)
