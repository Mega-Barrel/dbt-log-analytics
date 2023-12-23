"""Kafka Consumer"""

import os
import json
import time

from dotenv import load_dotenv
from kafka import KafkaConsumer # pylint: disable=E0611
from python_path import PythonPath
from google.api_core.exceptions import ClientError


path = os.path.join("..", "..")

with PythonPath(path, relative_to = __file__):
    from backend.db.tables import create_table
    from backend.common.dbt_logger import logger
    from backend.db.bq_client import client_oauth #pylint: disable=E0401
    from backend.db.insert_data import insert_data_json

def main(client, consumer, kafka_topic, table):
    """Main method to start consumer"""
    logger.info('Starting kafka consumer')
    print('Starting kafka consumer')
    while True:
        logger.info("Consuming data from %s topic", kafka_topic)
        raw_data = next(consumer)
        data = raw_data.value

        # Insert data to table
        json_data = {
            'date': data['date'],
            'ip_address': data['ip_address'],
            'user_agent': data['user_agent'],
            'request_type': data['request_type'],
            'status_code': data['status_code'],
            'username': data['username']
        }
        try:
            insert_data_json(client, table, json_data)
        except ClientError as error:
            print(f'Error occured while inserting record, More detail on error: {error}')

if __name__ == '__main__':
    load_dotenv()
    # Table name
    TABLE_NAME = 'raw_logs'
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

    # Create raw_logs_data table
    bq_client = client_oauth('dbt_bigquery_creds.json')
    project_name = os.environ.get('PROJECT')
    project_id = os.environ.get('PROJECT_ID')
    dataset = project_name + '.' + project_id + '.' + TABLE_NAME

    # creating raw_logs table
    create_table(
        client=bq_client,
        table_name=TABLE_NAME,
        dataset_name=project_id
    )
    logger.info('Slepping for 2 seconds....')
    time.sleep(2)

    # call the main funtion
    main(
        client=bq_client,
        consumer=kf_consumer,
        kafka_topic=KAFKA_TOPIC,
        table=dataset
    )
