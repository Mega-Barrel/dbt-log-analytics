'''List all tables'''

import os
from python_path import PythonPath

from dotenv import load_dotenv

path = os.path.join("..", "..")

with PythonPath(path, relative_to = __file__):
    from bq_client import client_oauth #pylint: disable=E0401

def list_tables(client, dataset_id):
    """Method to list all the Project tables"""
    tables = client.list_tables(dataset_id)

    print(f"Tables contained in '{dataset_id}':")
    for table in tables:
        print(f"\t{table.project}.{table.dataset_id}.{table.table_id}")

if __name__ == '__main__':
    load_dotenv()

    bq_client = client_oauth('dbt_bigquery_creds.json')
    project = os.environ.get('PROJECT')
    project_id = os.environ.get('PROJECT_ID')

    # dataset id
    DATASET_ID = project + '.' + project_id
    list_tables(client=bq_client, dataset_id=DATASET_ID)
