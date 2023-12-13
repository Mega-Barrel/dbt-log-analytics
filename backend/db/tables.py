'''Creating Raw log data table'''

from google.cloud import bigquery #pylint: disable=E0401
from google.cloud.exceptions import NotFound

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
