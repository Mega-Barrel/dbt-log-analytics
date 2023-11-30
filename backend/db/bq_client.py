''' BigQuery Client Connection'''

import os
from google.cloud import bigquery #pylint: disable=E0401

def client_oauth(file_path):
    """Authenticate BigQuery Service Account credentials"""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = file_path

    # Create client object
    client = bigquery.Client()

    # return obj
    return client

# for data in query_job.result():
#     print(data)
#     print()
