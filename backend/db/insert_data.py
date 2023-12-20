"""Push kafka consumer data to BQ tables"""

import os
from python_path import PythonPath
from google.cloud.bigquery.table import Table
from google.api_core.exceptions import ClientError

path = os.path.join("..", "..")

with PythonPath(path, relative_to = __file__):
    from backend.common.dbt_logger import logger
    # from backend.db.bq_client import client_oauth #pylint: disable=E0401

def insert_data_json(client, project_id, data):
    """
    Inserts data into a BigQuery table using the insert_rows_json method.

    Args:
        client: A BigQuery client object.
        dataset_id: The ID of the BigQuery dataset.
        table_id: The ID of the BigQuery table.
        data: A list of dictionaries representing the data rows. Each dictionary
            should have keys corresponding to the table schema columns.

    Returns:
        None
    """

    try:
        # Prepare the full table reference
        table_id = Table.from_string(project_id)

        # Insert the rows as JSON
        errors = client.insert_rows_json(table_id, [data])

        # Check for errors
        if errors:
            # print("Encountered errors while inserting data: %s", errors)
            logger.info("Encountered errors while inserting data: %s", errors)
        else:
            # print("Inserted %s rows into table %s", len(rows_to_insert), table_name)
            logger.info("Inserted raw API data into table_id %s", table_id)
    except ClientError as error:
        # print("Error occurred while inserting data: %s", error)
        logger.error("Error occurred while inserting data: %s", error)
