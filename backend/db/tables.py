'''Creating Raw log data table'''

import os
from python_path import PythonPath

from google.cloud import bigquery #pylint: disable=E0401
from google.cloud.exceptions import NotFound

path = os.path.join("..", "..")

with PythonPath(path, relative_to = __file__):
    from bq_client import client_oauth #pylint: disable=E0401

def create_table(client, table_name, dataset_name):
    """Create table if not exist"""

    # dataset reference
    dataset_ref = client.dataset(dataset_name)
    # Table reference
    table_ref = dataset_ref.table(table_name)

    try:
        table = client.get_table(table_ref)
        print(f'table {table} already exists.')
    except NotFound:
        schema = [
            bigquery.SchemaField(
                'api_request_number', 
                'INTEGER', 
                mode='REQUIRED',
                description='Test description'
            )
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f'table {table.table_id} created.')
    return table

if __name__ == '__main__':
    bq_client = client_oauth('dbt_bigquery_creds.json')
    DATASET_ID = 'dbt_log_analytics'
    TABLE_NAME = 'raw_logs'
    tlb = create_table(client=bq_client, dataset_name=DATASET_ID, table_name=TABLE_NAME)
