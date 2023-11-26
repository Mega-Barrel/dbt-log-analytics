"""Kafka Consumer"""

import json
import os

from python_path import PythonPath
from kafka import KafkaConsumer # pylint: disable=E0611

path = os.path.join("..", "..")

with PythonPath(path, relative_to = __file__):
    from backend.common.dbt_logger import logger

# kafka server
kafka_server = [ "localhost:29092" ]

# kafka topic
KAFKA_TOPIC = "api_logs"

consumer = KafkaConsumer(
    bootstrap_servers = kafka_server,
    value_deserializer = json.loads,
    auto_offset_reset = "latest",
)

consumer.subscribe(topics=KAFKA_TOPIC)

logger.info('Starting kafka consumer')
print('Starting kafka consumer')
while True:
    logger.info("Consuming data from %s topic", KAFKA_TOPIC)
    data = next(consumer)
    print(data)
    print()
