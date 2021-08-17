import os
import json
import logging

from kafka import KafkaProducer


def connect_kafka_producer():
    _producer = None
    kafka_advertised_port = os.getenv("KAFKA_ADVERTISED_PORT", "9092")
    kafka_advertised_listeners = os.getenv("KAFKA_ADVERTISED_LISTENERS").split(",")
    bootstrap_servers = [
        "{}:{}".format(kafka_advertised_listener, kafka_advertised_port)
        for kafka_advertised_listener in kafka_advertised_listeners
    ]
    try:
        _producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            api_version=(0, 10),
            acks=1,
            retries=1,
        )
    except Exception as ex:
        logging.error("Exception while connecting Kafka: {}".format(str(ex)))
    finally:
        return _producer


def on_kafka_send_error(excp):
    logging.error(str(excp))
