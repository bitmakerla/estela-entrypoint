import json
from datetime import date, datetime, timedelta

import requests
from estela_queue_adapter import get_producer_interface


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


def update_job(
    job_url,
    auth_token,
    status,
    lifespan=timedelta(seconds=0),
    total_bytes=0,
    item_count=0,
    request_count=0,
    proxy_usage_data={},
):
    requests.patch(
        job_url,
        data={
            "status": status,
            "lifespan": lifespan,
            "total_response_bytes": total_bytes,
            "item_count": item_count,
            "request_count": request_count,
            "proxy_usage_data": json.dumps(proxy_usage_data),
        },
        headers={"Authorization": "Token {}".format(auth_token)},
    )


producer = get_producer_interface()
