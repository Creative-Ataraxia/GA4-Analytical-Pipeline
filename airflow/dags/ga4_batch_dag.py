import os, io, json, shutil, logging
from datetime import datetime, timedelta
import logging
import textwrap

import boto3
import s3fs
import pandas as pd
import numpy as np
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# if you chose to download the spark-aws jars locally, switch the `packages` flag in spark-submit to `jars`

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
    'base_ga4_events': {'date': '2020-11-01', 'days': '1'}, # for ingestion, day counts backwards; inclusive
    'stg_ga4_events': {'date': '2020-11-01', 'days': '1'}   # for transformations, day counts forward; inclusive
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

def load_stg_events_to_postgres(**context):
    """
    Load all partitions of stg_ga4_events Parquet into Postgres staging table.
    - Reads entire s3a://ga4-bronze/stg_ga4_events dataset
    - Injects partition column event_date_dt
    - JSON-encodes nested columns for JSONB storage
    - Creates table if missing, truncates before load
    - Bulk loads via COPY for performance
    - Logs each key step
    """
    
    # 1) Read full Parquet dataset into 1 giant pandas df; adjust your container/airflow worker mem
    fs = s3fs.S3FileSystem(   
        client_kwargs={'endpoint_url': 'http://minio:9000'},
        key='minio',
        secret='miniopass',
        use_ssl=False
    )
    path = "ga4-bronze/stg_ga4_events"
    logging.info(f"Reading partitions from bucket: {path}")

    df = pd.read_parquet( # unlike spark, pandas does not read the event_date_dt (from DB path) back into the data
        path=path,
        filesystem=fs,
        engine='pyarrow'
    )
    logging.info(f"Loaded {len(df)} rows and {len(df.columns)} columns")

    # 3) JSON-encode nested columns
    def safe_serialize(val):
        if val is None:
            return None
        if isinstance(val, np.ndarray):
            val = val.tolist()
        return json.dumps(val)

    for c in ['ecommerce', 'items', 'event_params']:
        if c in df.columns:
            logging.info(f"JSON-encoding column '{c}'")
            df[c] = df[c].apply(safe_serialize)
        else:
            logging.info(f"Column '{c}' not found, skipping JSON encode")

    # 4) Connect to Postgres
    logging.info("Connecting to Postgres...")
    hook = PostgresHook(postgres_conn_id='airflow-postgres-conn') # connects to the db set in airflow's connection profile, here ga4_data
    conn = hook.get_conn() # besure the DB (ga4_data) already exists in postgres
    cur = conn.cursor()

    

    # 5) Create staging table if not exists
    logging.info("Ensuring stg_ga4_events table exists in DB")
    cur.execute('''
    CREATE TABLE IF NOT EXISTS stg_ga4_events (
      event_key TEXT PRIMARY KEY,
      _event_date_dt DATE,
      event_timestamp BIGINT,
      event_name TEXT,
      privacy_info_analytics_storage INT,
      privacy_info_ads_storage INT,
      privacy_info_uses_transient_token TEXT,
      user_id TEXT,
      user_pseudo_id TEXT,
      user_first_touch_timestamp BIGINT,
      user_ltv_revenue DOUBLE PRECISION,
      user_ltv_currency TEXT,
      device_category TEXT,
      device_mobile_brand_name TEXT,
      device_mobile_model_name TEXT,
      device_mobile_marketing_name TEXT,
      device_mobile_os_hardware_model BIGINT,
      device_operating_system TEXT,
      device_operating_system_version TEXT,
      device_vendor_id BIGINT,
      device_advertising_id BIGINT,
      device_language TEXT,
      device_is_limited_ad_tracking TEXT,
      device_time_zone_offset_seconds BIGINT,
      geo_continent TEXT,
      geo_country TEXT,
      geo_region TEXT,
      geo_city TEXT,
      geo_sub_continent TEXT,
      geo_metro TEXT,
      app_info_id TEXT,
      app_info_version TEXT,
      app_info_install_store TEXT,
      app_info_firebase_app_id TEXT,
      app_info_install_source TEXT,
      user_campaign TEXT,
      user_medium TEXT,
      user_source TEXT,
      stream_id BIGINT,
      platform TEXT,
      ecommerce JSONB,
      items JSONB,
      session_id BIGINT,
      page_location TEXT,
      session_number INT,
      engagement_time_msec BIGINT,
      page_title TEXT,
      page_referrer TEXT,
      event_source TEXT,
      event_medium TEXT,
      event_campaign TEXT,
      event_content TEXT,
      event_term TEXT,
      session_engaged BIGINT,
      is_page_view INT,
      is_purchase INT,
      property_id BIGINT,
      event_params JSONB,
      client_key TEXT,
      session_key TEXT,
      session_partition_key TEXT,
      original_page_location TEXT,
      original_page_referrer TEXT,
      page_path TEXT,
      page_hostname TEXT,
      page_query_string TEXT,
      page_key TEXT,
      page_engagement_key TEXT
    );
    ''')
    conn.commit()
    logging.info("Table check/creation complete")

    # 6) Truncate table for idempotent load
    logging.info("Truncating stg_ga4_events table in case old data already exists")
    cur.execute("TRUNCATE TABLE stg_ga4_events;")
    conn.commit()

    # Ensure BIGINT fields are cast to integer so Postgres COPY won't fail
    bigint_cols = [
        'event_timestamp',
        'user_first_touch_timestamp',
        'device_time_zone_offset_seconds',
        'device_vendor_id',
        'device_advertising_id',
        'stream_id',
        'session_id',
        'engagement_time_msec',
        'session_engaged',
        'property_id'
    ]
    for col_name in bigint_cols:
        if col_name in df.columns:
            df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64') # NaN allowed (int64 does not)

    # 7) Bulk load via COPY
    logging.info("Starting bulk COPY into Postgres")
    buffer = io.StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)
    cols = ','.join(df.columns)
    copy_sql = f"COPY stg_ga4_events ({cols}) FROM STDIN WITH (FORMAT CSV)"
    cur.copy_expert(copy_sql, buffer)
    conn.commit()
    logging.info(f"Loaded {len(df)} rows into stg_ga4_events table")

    # 8) Clean up
    cur.close()
    conn.close()
    logging.info("Postgres connection closed")

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
        # jars=','.join([
        #     '/opt/spark/libs/hadoop-aws-3.4.1.jar',
        #     '/opt/spark/libs/aws-java-sdk-bundle-1.12.655.jar',
        #     '/opt/spark/libs/aws-sdk-bundle-2.24.6.jar',
        #     '/opt/spark/libs/wildfly-openssl-1.1.3.Final.jar'
        # ]),
        packages = ','.join([
            'org.apache.hadoop:hadoop-aws:3.4.1',
            'com.amazonaws:aws-java-sdk-bundle:1.12.655',
            'software.amazon.awssdk:bundle:2.24.6',
            'org.wildfly.openssl:wildfly-openssl:1.1.3.Final'
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
        # jars=','.join([
        #     '/opt/spark/libs/hadoop-aws-3.4.1.jar',
        #     '/opt/spark/libs/aws-java-sdk-bundle-1.12.655.jar',
        #     '/opt/spark/libs/aws-sdk-bundle-2.24.6.jar',
        #     '/opt/spark/libs/wildfly-openssl-1.1.3.Final.jar'
        # ]),
        packages = ','.join([
            'org.apache.hadoop:hadoop-aws:3.4.1',
            'com.amazonaws:aws-java-sdk-bundle:1.12.655',
            'software.amazon.awssdk:bundle:2.24.6',
            'org.wildfly.openssl:wildfly-openssl:1.1.3.Final'
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

    load_stg_events_to_pg_task = PythonOperator(
        task_id='load_stg_events_to_pg',
        python_callable=load_stg_events_to_postgres
    )

    load_stg_events_to_pg_task.doc_md = textwrap.dedent(
        """\
        Load all partitions of stg_ga4_events Parquet into Postgres staging table.
        * Reads entire s3a://ga4-bronze/stg_ga4_events dataset
        * Injects partition column event_date_dt
        * JSON-encodes nested columns for JSONB storage
        * Creates table if missing, truncates before load
        * Bulk loads via COPY for performance
        * Logs each key step
        """
    )

# Define the DAG
    _ = extract_raw_task >> load_raw_to_minio_task >> base_ga4_events_task >> stg_ga4_events_task >> load_stg_events_to_pg_task
