from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from pendulum import datetime
import os
from google.oauth2 import service_account
from google.cloud import bigquery

@dag(
    schedule=None,
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=["retail", "analytics", "bq_load"],
)
def retail_bucket_to_bq():

    @task
    def load_from_bucket_to_bq():
        credentials_path = "/usr/local/airflow/include/service_account.json"
        credentials_abs_path = os.path.abspath(credentials_path)

        credentials = service_account.Credentials.from_service_account_file(credentials_abs_path)

        client = bigquery.Client(credentials=credentials, project="ecommerce-analysis-453419")

        bucket_name = "ecommerce-bucket-27"
        project_id = "ecommerce-analysis-453419"
        dataset_id = "online_retail_27"

        tables_to_load = [
            {
                "table_id": f"{project_id}.{dataset_id}.online_retail_27_cleaned",
                "source_uri": f"gs://{bucket_name}/retail/cleaned_data/*.parquet",
                "write_disposition": "WRITE_TRUNCATE",
            },
            {
                "table_id": f"{project_id}.{dataset_id}.rfm_online_retail_27",
                "source_uri": f"gs://{bucket_name}/retail/rfm_data/*.parquet",
                "write_disposition": "WRITE_TRUNCATE",
            }
        ]

        results = {}

        for table_info in tables_to_load:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=table_info["write_disposition"],
            )

            load_job = client.load_table_from_uri(
                table_info["source_uri"],
                table_info["table_id"],
                job_config=job_config,
            )

            load_job.result()

            table_ref = client.get_table(table_info["table_id"])
            results[table_info["table_id"]] = f"Loaded {table_ref.num_rows} rows to {table_info['table_id']}"

        return results

    @task
    def log_loading_results(results):
        for table_id, message in results.items():
            print(f"Table {table_id}: {message}")

        return "Loading completed successfully"

    loading_results = load_from_bucket_to_bq()
    log_loading_results(loading_results)

retail_bucket_to_bq_dag = retail_bucket_to_bq()