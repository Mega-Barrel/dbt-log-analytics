'''Creating Raw log data table'''
import os

from python_path import PythonPath

from google.cloud import bigquery #pylint: disable=E0401
from google.cloud.exceptions import NotFound

path = os.path.join("..", "..")

with PythonPath(path, relative_to = __file__):
    from backend.common.dbt_logger import logger

def create_table(client, table_name, dataset_name):
    """Create table if not exist"""

    # dataset reference
    dataset_ref = client.dataset(dataset_name)
    # Table reference
    table_ref = dataset_ref.table(table_name)

    try:
        table = client.get_table(table_ref)
        logger.info('table %s already exists.', table)
    except NotFound:
        schema = [
            bigquery.SchemaField(
                'date',
                'TIMESTAMP',
                mode='REQUIRED',
                description='Date when API was used'
            ),
            bigquery.SchemaField(
                'ip_address', 
                'STRING', 
                mode='REQUIRED',
                description='Fake ip address of user'
            ),
            bigquery.SchemaField(
                'user_agent',
                'STRING',
                mode='REQUIRED',
                description='Used to identify identify the application, operating system of user'
            ),
            bigquery.SchemaField(
                'request_type',
                'STRING',
                mode='REQUIRED',
                description='API request type'
            ),
            bigquery.SchemaField(
                'status_code',
                'STRING',
                mode='REQUIRED',
                description='Statuscode of API request'
            ),
            bigquery.SchemaField(
                'api_req_no', 
                'STRING', 
                description='API request number'
            ),
            bigquery.SchemaField(
                'username', 
                'STRING', 
                description='Username'
            )
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        logger.info('table %s created.', table.table_id)
