"""Kafka Consumer"""

import json
import os

from python_path import PythonPath
from kafka import KafkaConsumer # pylint: disable=E0611

path = os.path.join("..", "..")

with PythonPath(path, relative_to = __file__):
    from bq_client import client_oauth #pylint: disable=E0401
    from backend.common.dbt_logger import logger
    from backend.db.insert_data import insert_raw_data

def main(consumer, kafka_topic):
    """Main method to start consumer"""
    logger.info('Starting kafka consumer')
    print('Starting kafka consumer')
    while True:
        logger.info("Consuming data from %s topic", kafka_topic)
        data = next(consumer)

        # Check to skip empty data
        if data is None:
            continue
        else:
            # Insert data to table
            try:
                insert_raw_data('', '', data=data)
            except Exception as error:
                print(f'Error occured while inserting record, More detail on error: {error}')
            print(data)
            print()

if __name__ == '__main__':
    # kafka server
    kafka_server = [ "localhost:29092" ]
    # kafka topic
    KAFKA_TOPIC = "api_logs"
    # init
    kf_consumer = KafkaConsumer(
        bootstrap_servers = kafka_server,
        value_deserializer = json.loads,
        auto_offset_reset = "latest",
    )
    # Subscribe to kafka topic
    kf_consumer.subscribe(topics=KAFKA_TOPIC)
    # call the main funtion
    main(kf_consumer, KAFKA_TOPIC)
