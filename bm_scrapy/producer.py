import os
import json
import logging

from kafka import KafkaProducer


def connect_kafka_producer():
    _producer = None
    bootstrap_server = [
        '{}:{}'.format(
            os.getenv('KAFKA_ADVERTISED_HOST_NAME', 'localhost'),
            os.getenv('KAFKA_ADVERTISED_PORT', '9092')
        )
    ]
    try:
        _producer = KafkaProducer(
            bootstrap_servers=bootstrap_server,
            api_version=(0, 10),
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def on_kafka_send_error(excp):
    logging.getLogger('scrapy').error(str(excp))
