import os
import shutil
import logging
from datetime import datetime

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

def extract_raw_data(**kwargs):
    """
    Batch extract raw GA4 data from local source.

    - Reads local raw parquet files from /opt/airflow/raw-data/
    - Skips today's date (in case data may still be changing)
    - Checks if file already exists in MinIO bucket (ga4-bronze); skips if so
    - Copies new files to /opt/airflow/tmp/
    """

    # Task Configs
    bucket = "ga4-bronze"
    local_raw_dir = "/opt/airflow/raw-data"
    tmp_dir = "/opt/airflow/tmp"
    today = datetime.today().strftime("%y%m%d")
    extracted = 0

    # Connect to MinIO via boto3
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="miniopass",
        region_name="us-east-1",  # required dummy value
    )

    # Create /tmp/ if missing
    os.makedirs(tmp_dir, exist_ok=True)

    for fname in os.listdir(local_raw_dir):
        if not fname.endswith(".parquet"):
            logging.warning(f"skipping {fname} because it doesn't ends in parquet")
            continue

        # skip today's file
        if today in fname:
            logging.warning(f"skipping {fname} because it's today's ongoing data")
            continue

        local_path = os.path.join(local_raw_dir, fname)
        object_key = f"raw/{fname}"  # destination key in bucket

        # check if already in S3 (MinIO)
        try:
            s3.head_object(Bucket=bucket, Key=object_key)
            logging.info(f"Skipped {fname} because it already exists in destination bucket")
            continue
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise  # real error → fail fast

        # copy to /tmp/
        dest_path = os.path.join(tmp_dir, fname)
        shutil.copyfile(local_path, dest_path)
        extracted += 1
        logging.info(f"Extracted {fname} to {dest_path}")

    logging.info(f"[Complete] Extracted {extracted} new GA4 partition(s) to {tmp_dir}")



# Declare DAG with Tasks
with DAG(
        dag_id='ga4_batch_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_raw',
        python_callable=extract_raw_data
    )

# Define the DAG
    extract_task