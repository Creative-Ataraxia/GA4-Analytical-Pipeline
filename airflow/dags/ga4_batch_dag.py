import os
import shutil
import logging
from datetime import datetime, timedelta
import textwrap

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 11, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# dag_params
raw_bucket = "ga4-bronze"
local_raw_dir = "/opt/airflow/raw-data"
tmp_dir = "/opt/airflow/tmp"
spark_job_args = {
    'base_ga4_events': {'date': '2020-11-01', 'days': '1'},
    'stg_ga4_events': {'date': '2020-11-01', 'days': '1'}
}


def extract_raw_data(**kwargs):
    """
    Batch extract raw GA4 data from local source.
    """

    # Task Configs
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
            s3.head_object(Bucket=raw_bucket, Key=object_key)
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

    logging.info(f"[Completed] Extracted {extracted} new GA4 partition(s) to {tmp_dir}")

def load_raw_to_minio(**kwargs):
    """
    Upload extracted GA4 parquet files from /opt/airflow/tmp to MinIO. (Can be resued with S3)
    """

    uploaded = 0

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="miniopass",
        region_name="us-east-1",
    )

    # ensure bucket exists (or is already owned by us)
    try:
        s3.head_bucket(Bucket=raw_bucket)
        logging.info(f"Bucket '{raw_bucket}' already exists.")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=raw_bucket)
            logging.info(f"Created bucket '{raw_bucket}'.")
        else:
            raise   # any other error is fatal

    # upload every parquet file from /tmp/
    for fname in os.listdir(tmp_dir):
        if not fname.endswith(".parquet"):
            continue

        file_path   = os.path.join(tmp_dir, fname)
        object_key  = f"raw/{fname}"

        # skip if object already exists
        try:
            s3.head_object(Bucket=raw_bucket, Key=object_key)
            logging.info(f"Skipped upload; already in bucket: {object_key}")
            continue
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise

        s3.upload_file(file_path, raw_bucket, object_key)
        uploaded += 1
        logging.info(f"Uploaded {fname} to s3://{raw_bucket}/{object_key}")

    if uploaded == 0: logging.info("No data found in /tmp/ folder!")

    logging.info(f"[Completed] Uploaded {uploaded} new daily GA4 partition(s) into MinIO bucket '{raw_bucket}/raw'.")


# Declare DAG with Tasks
with DAG(
        dag_id='ga4_batch_dag',
        default_args=default_args,
        schedule=timedelta(days=1),
        description="GA4 Batched Pipeline",
        catchup=False,
        tags=["main"],
) as dag:
    
    extract_raw_task = PythonOperator(
        task_id='extract_raw',
        python_callable=extract_raw_data
    )

    extract_raw_task.doc_md = textwrap.dedent(
        """\
    ### Batch extract raw GA4 data from local source.

    * Reads local raw parquet files from /opt/airflow/raw-data/
    * Skips today's date (in case data may still be changing)
    * if file already exists in MinIO bucket "ga4-bronze", skip
    * Copies new files to /opt/airflow/tmp/
    """
    )

    load_raw_to_minio_task = PythonOperator(
        task_id='load_raw_to_minio',
        python_callable=load_raw_to_minio
    )

    load_raw_to_minio_task.doc_md = textwrap.dedent(
        """\
    Upload extracted GA4 parquet files from /opt/airflow/tmp to MinIO.
    * Task can be reused with S3
    * Bucket: ga4-bronze
    * Key: raw/<filename>
    * Idempotent: skips objects that already exist
    """
    )

    base_ga4_events_task = SparkSubmitOperator(
        task_id='base_ga4_events',
        conn_id='airflow-spark-conn',
        application='/opt/spark-apps/staging/base_ga4_events.py',
        name='base_ga4_events_job',
        application_args=[
            f"{spark_job_args['base_ga4_events']['date']}",
            f"{spark_job_args['base_ga4_events']['days']}"
        ], # days count backwards, inclusive
        jars=','.join([
            '/opt/spark/libs/hadoop-aws-3.4.1.jar',
            '/opt/spark/libs/aws-java-sdk-bundle-1.12.655.jar',
            '/opt/spark/libs/aws-sdk-bundle-2.24.6.jar',
            '/opt/spark/libs/wildfly-openssl-1.1.3.Final.jar'
        ]),
        env_vars={
            'PYTHONPATH': '/opt/spark-apps',
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64"
        }
    )

    base_ga4_events_task.doc_md = textwrap.dedent(
        """\
        Spark batch job to implement the GA4 base events model:
        * Reads raw GA4 Parquet files from MinIO (s3a)
        * Applies base_select_source and base_select_renamed transforms
        * Deduplicates events by key+payload
        * Writes out partitioned Parquet back to MinIO
        """
    )

    stg_ga4_events_task = SparkSubmitOperator(
        task_id='stg_ga4_events',
        conn_id='airflow-spark-conn',
        application='/opt/spark-apps/staging/stg_ga4_events.py',
        name='stg_ga4_events_job',
        application_args=[
            f"{spark_job_args['stg_ga4_events']['date']}",
            f"{spark_job_args['stg_ga4_events']['days']}"
        ],
        jars=','.join([
            '/opt/spark/libs/hadoop-aws-3.4.1.jar',
            '/opt/spark/libs/aws-java-sdk-bundle-1.12.655.jar',
            '/opt/spark/libs/aws-sdk-bundle-2.24.6.jar',
            '/opt/spark/libs/wildfly-openssl-1.1.3.Final.jar'
        ]),
        env_vars={
            'PYTHONPATH': '/opt/spark-apps',
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64"
        }
    )

    stg_ga4_events_task.doc_md = textwrap.dedent(
        """\
        Spark job to build GA4 staging events model (stg_ga4_events)
        * Reads transformed base events (s3a://ga4-bronze/base_ga4_events)
        * Adds client, session, event keys; detects gclid; cleans URLs
        * Extracts hostname and query string; computes page keys
        * Writes partitioned by event_date_dt to s3a://ga4-bronze/stg_ga4_events
        """
    )
# Define the DAG
    _ = extract_raw_task >> load_raw_to_minio_task >> base_ga4_events_task >> stg_ga4_events_task


