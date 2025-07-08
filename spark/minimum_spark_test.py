"""
minimum_spark_test.py
• connects to the Spark master
• reads any parquet file from MinIO
• prints row-count + first 3 rows
"""

import logging, sys
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# MinIO (S3-compatible) Configurations
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "miniopass"
RAW_PATH = "s3a://ga4-bronze/raw/events_*.parquet"

spark = None

try:
    spark = (
        SparkSession.builder
        .appName("is-spark-working")
        .master("spark://spark-master:7077")
        # S3A options
        .config("spark.hadoop.fs.s3a.endpoint",              MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",            MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",            MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
        # Disable Kerberos
        .config("spark.driver.extraJavaOptions",   "-Dhadoop.security.authentication=simple")
        .config("spark.executor.extraJavaOptions", "-Dhadoop.security.authentication=simple")
        .getOrCreate()
    )

    logging.info("Reading GA4 parquet files from %s …", RAW_PATH)
    df = spark.read.parquet(RAW_PATH)

    if df.rdd.isEmpty():
        raise ValueError("No rows found in source files.")

    logging.info("Row count: %d", df.count())
    df.show(3, truncate=False)
    logging.info("spark is working!")

except AnalysisException as e:
    logging.error("Spark DataFrame operation failed: %s", e)
    sys.exit(1)
except Exception as e:
    logging.error("Spark batch processing failed: %s", e)
    sys.exit(1)
finally:
    if spark is not None:
        spark.stop()
