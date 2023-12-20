''' BigQuery Client Connection'''

import os
from google.cloud import bigquery #pylint: disable=E0401
from google.oauth2 import service_account

def client_oauth(file_path):
    """Authenticate BigQuery Service Account credentials"""
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = file_path

    credentials = service_account.Credentials.from_service_account_file(file_path)

    # Extract the project ID from the credentials
    project_id = credentials.project_id
    # Create client object
    client = bigquery.Client(project=project_id)

    # return obj
    return client
