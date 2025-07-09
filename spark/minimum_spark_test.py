"""
minimum_spark_test.py
• connects to the Spark master
• reads any parquet file from MinIO
• prints row-count + first 3 rows
"""

import logging, sys
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.base import AnalysisException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
spark = None

try:
    spark = (
        SparkSession.builder
        .appName("spark-minio-smoketest")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.endpoint",              "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",            "minio")
        .config("spark.hadoop.fs.s3a.secret.key",            "miniopass")
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .getOrCreate()
    )

    logging.info("Connected to MinIO, reading a single file as a test...")
    df = spark.read.parquet("s3a://ga4-bronze/raw/events_20210131.parquet")

    if df.rdd.isEmpty():
        raise ValueError("No rows found in source files.")

    logging.info("File events_20210131 has %d rows", df.count())
    logging.info("Spark is working!")

except AnalysisException as e:
    logging.error("Spark DataFrame operation failed: %s", e)
    sys.exit(1)
except Exception as e:
    logging.error("Spark batch processing failed: %s", e)
    sys.exit(1)
finally:
    if spark is not None:
        spark.stop()


# note: must use the correct version of hadoop-aws with the underlying hadoop version of spark;
# e.g., apache/spark:4.0.0 built for hadoop > 3.4+; so we need hadoop-aws:3.4.1