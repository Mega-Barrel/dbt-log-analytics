"""Kafka producer"""

# System packages
import os
import time
import json
from random import choice
from datetime import (
    datetime,
    timedelta
)

# Installed packages
from faker import Faker
from kafka import KafkaProducer # pylint: disable=E0611
from python_path import PythonPath

path = os.path.join("..", "..")

with PythonPath(path, relative_to = __file__):
    from backend.common.dbt_logger import logger

fake = Faker()

# kafka server
kafka_server = [ "localhost:29092" ]

# kafka topic
KAFKA_TOPIC = "api_logs"

# kafka producer
producer = KafkaProducer(
    bootstrap_servers = kafka_server,
    value_serializer = lambda x: json.dumps(x).encode("utf-8"),
)

# data dictionary for request and statuscode
dictionary = {
    'request': [
        'GET', 'POST', 'PUT', 'DELETE'
    ],
    'statuscode': [
        '303', '404', '500', 
        '403', '502', '304',
        '200'
    ]
}

# Define the start and end dates
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 11, 24)

# Generate API log data
API_REQ_NO = 1

# Loop through the dates minute by minute
while start_date <= end_date:

    # Get the user agent string
    user_agent = fake.user_agent()
    ip_addr = fake.ipv4()
    request_type = choice(dictionary['request'])
    status_code_resp = choice(dictionary['statuscode'])

    data = {
        'ip_address': ip_addr,
        'user_agent': user_agent,
        'request_type': request_type,
        'status_code': status_code_resp,
        'meta_data': {
            'timestamp': str(start_date),
            'api_req_no': API_REQ_NO
        }
    }

    logger.info("Fake data generated.")

    # Send data to defined Kafka topic
    producer.send(KAFKA_TOPIC, data)
    producer.flush()

    # Increment the date and time by one minute
    start_date += timedelta(minutes=1)
    API_REQ_NO += 1

    logger.info("Data Produced with API_REQ_NO: %s", API_REQ_NO)

    # sleep for 1 second
    time.sleep(5)
    logger.info("Sleeping for 5 second")
