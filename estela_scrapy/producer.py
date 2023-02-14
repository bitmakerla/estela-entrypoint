import logging
import os

from estela_queue_adapter import get_producer_interface


def parse_queue_params():
    prefix = "QUEUE_PLATFORM_"
    env_vars = [var for var in os.environ if var.startswith(prefix)]
    params = {var[len(prefix) :].lower(): os.environ[var] for var in env_vars}
    return params


producer = get_producer_interface(os.getenv("QUEUE_PLATFORM"), **parse_queue_params())
if producer.get_connection():
    logging.debug("Successful connection to queue platform.")
else:
    raise Exception("Could not connect to the queue platform.")
