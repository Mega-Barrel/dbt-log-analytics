'''Push kafka consumer data to BQ tables'''

from google.cloud import bigquery #pylint: disable=E0401

def insert_raw_data(client, dataset_id, data):
    """Method to push consumer data to raw_log_data"""
    print(client, dataset_id)
    print(data)
