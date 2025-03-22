from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from pendulum import datetime
import os
from google.oauth2 import service_account
import json
from pyspark.sql import SparkSession
from retail_pipeline import create_pipeline, run_pipeline, rfm_pipeline


@dag(
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=["retail", "analytics", "setup"],
)
def retail_initial_setup():
    @task
    def setup_spark():
        credentials_path = "/usr/local/airflow/include/service_account.json"
        credentials_abs_path = os.path.abspath(credentials_path)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_abs_path

        if "JAVA_HOME" not in os.environ:
            os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

        spark = SparkSession.builder \
            .appName("RetailInitialSetup") \
            .master("local[*]") \
            .config("spark.jars",
                    "/opt/spark_jars/spark-3.5-bigquery-0.42.1.jar,/opt/spark_jars/gcs-connector-hadoop3-latest.jar") \
 \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_abs_path) \
            .getOrCreate()

        return {"spark": "initialized"}


    @task
    def process_all_historical_data(spark_context):
        credentials_path = "/usr/local/airflow/include/service_account.json"
        credentials_abs_path = os.path.abspath(credentials_path)

        spark = SparkSession.builder \
            .appName("RetailInitialSetup") \
            .master("local[*]") \
            .config("spark.jars",
                    "/opt/spark_jars/spark-3.5-bigquery-0.42.1.jar,/opt/spark_jars/gcs-connector-hadoop3-latest.jar") \
 \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_abs_path) \
            .getOrCreate()

        raw_data = spark.read.format("bigquery") \
                    .option("credentialsFile", credentials_abs_path) \
                    .option("parentProject", "ecommerce-analysis-453419") \
                    .option("viewsEnabled", "true") \
                    .option("table", "ecommerce-analysis-453419.online_retail_27.online_retail_27_raw") \
                    .load()

        pipeline = create_pipeline()
        cleaned_data = run_pipeline(pipeline, raw_data)

        spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_abs_path)
        spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

        bucket_name = "ecommerce-bucket-27"
        cleaned_data.write \
            .mode("overwrite") \
            .parquet(f"gs://{bucket_name}/retail/cleaned_data/") \

        rfm_data = rfm_pipeline(cleaned_data)
        rfm_data.write \
            .mode("overwrite") \
            .parquet(f"gs://{bucket_name}/retail/rfm_data/")

        return {
            "processed_rows": raw_data.count(),
            "gcs_cleaned_path": f"gs://{bucket_name}/retail/cleaned_data/",
            "gcs_rfm_path": f"gs://{bucket_name}/retail/rfm_data/"
        }


    spark_ready = setup_spark()
    processed = process_all_historical_data(spark_ready)

retail_initial_setup_dag = retail_initial_setup()