'''List all tables'''

dataset_id = 'analytics-engineering-101.dbt_log_analytics'

tables = client_oauth('dbt_bigquery_creds.json').list_tables(dataset_id)

print(f"Tables contained in '{dataset_id}':")
for table in tables:
    print(f"{table.project}.{table.dataset_id}.{table.table_id}")
