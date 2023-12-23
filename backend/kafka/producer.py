"""Kafka producer"""

# System packages
import os
import time
import json
from random import choice, randint
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
end_date = datetime(2023, 6, 20)

# Loop through the dates minute by minute
while start_date <= end_date:

    # Get the user agent string
    user_name = fake.user_name()
    user_agent = fake.user_agent()
    ip_addr = fake.ipv4()
    request_type = choice(dictionary['request'])
    status_code_resp = choice(dictionary['statuscode'])

    data = {
        'date': str(start_date),
        'ip_address': ip_addr,
        'user_agent': user_agent,
        'request_type': request_type,
        'status_code': status_code_resp,
        'username': user_name
    }

    logger.info("Fake data generated.")

    # Send data to defined  Kafka topic
    producer.send(KAFKA_TOPIC, data)
    producer.flush()

    logger.info("Data Produced by Kafka Producer..")

    # sleep for x second
    random_seconds = randint(850, 1200)

    # Increment the date and time by one minute
    start_date += timedelta(seconds=random_seconds)

    logger.info("Sleeping for 2 second")
    time.sleep(2)
