import json
import logging
import time
from datetime import date, datetime, timedelta

import requests
from estela_queue_adapter import get_producer_interface

logger = logging.getLogger(__name__)

UPDATE_JOB_ATTEMPTS = 3
UPDATE_JOB_TIMEOUT = 10


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
    error_reason=None,
):
    data = {
        "status": status,
        "lifespan": lifespan,
        "total_response_bytes": total_bytes,
        "item_count": item_count,
        "request_count": request_count,
        "proxy_usage_data": json.dumps(proxy_usage_data),
    }
    if error_reason:
        data["error_reason"] = error_reason
    for attempt in range(1, UPDATE_JOB_ATTEMPTS + 1):
        try:
            response = requests.patch(
                job_url,
                data=data,
                headers={"Authorization": "Token {}".format(auth_token)},
                timeout=UPDATE_JOB_TIMEOUT,
            )
            response.raise_for_status()
            return
        except requests.RequestException as e:
            logger.warning(
                "Could not update job status to %s (attempt %d/%d): %s",
                status, attempt, UPDATE_JOB_ATTEMPTS, e,
            )
            if attempt == UPDATE_JOB_ATTEMPTS:
                raise
            time.sleep(2 ** (attempt - 1))


producer = get_producer_interface()
