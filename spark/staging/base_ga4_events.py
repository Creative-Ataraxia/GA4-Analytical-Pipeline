# staging/base_ga4_events.py

"""
Spark batch job to implement the GA4 base events model:
  - Reads raw GA4 Parquet files from MinIO (s3a://ga4-bronze/raw/)
  - Automatically skips files already processed (based on partitions in s3a://ga4-bronze/base_ga4_events)
  - Applies base_select_source and base_select_renamed transforms
  - Deduplicates events by key+payload using row_number window function
  - Writes out partitioned Parquet by event_date_dt to s3a://ga4-bronze/base_ga4_events

Optional CLI Arguments:
  - used to select days to process, if not provided, scan all existing file from source
  - <start_date: YYYY-MM-DD>: If provided, start scanning raw files from this date
  - <days: int>: If provided, include this many prior days (inclusive)
    If neither is provided, process all raw files not yet written to output
    If only start_date is provided, process all newer unprocessed files from that date
    If only days is provided, process all new unprocessed files

Usage:
docker compose exec spark-master \
env PYTHONPATH=/opt/spark-apps \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--jars /opt/spark/libs/hadoop-aws-3.4.1.jar,\
/opt/spark/libs/aws-java-sdk-bundle-1.12.655.jar,\
/opt/spark/libs/aws-sdk-bundle-2.24.6.jar,\
/opt/spark/libs/wildfly-openssl-1.1.3.Final.jar \
/opt/spark-apps/staging/base_ga4_events.py [<start_date>] [<days>]

Note:
<days> counts back from <start_date>, inclusive
"""


from datetime import datetime, timedelta
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col, row_number
from pyspark.sql.window import Window

# import our reusable transforms
from common.base_select import base_select_source, base_select_renamed
from common.ga4_spark_utils import list_processed_dates, list_raw_dates


def normalize_date_str(date_str):
    parts = date_str.split("-")
    if len(parts) != 3:
        raise ValueError(f"Invalid date format: {date_str}")
    year, month, day = parts
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def main(start_date_str=None, days=None):
    # hard coded paths, based on airflow DAG design
    input_prefix = "s3a://ga4-bronze/raw"
    output_prefix = "s3a://ga4-bronze/base_ga4_events"

    spark = (
        SparkSession.builder
        .appName("ga4_base_events_job")
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "4g")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "miniopass")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    # 1) discover dates
    processed = list_processed_dates(spark, output_prefix)
    raw = list_raw_dates(spark, input_prefix)
    print("Dates in Processed:", sorted(processed))
    print("Dates in Raw:", sorted(raw))

    # compute candidate dates
    if start_date_str and days:
        start = datetime.fromisoformat(normalize_date_str(start_date_str)).date()
        candidate = {
            (start - timedelta(days=i)).isoformat()
            for i in range(days)
        }
    elif start_date_str:
        sd = datetime.fromisoformat(normalize_date_str(start_date_str)).date()
        candidate = {d for d in raw if datetime.fromisoformat(d).date() >= sd}
    else:
        candidate = raw

    # skip processed
    to_process = sorted(candidate - processed)
    processed = set(processed)
    raw = set(raw)
    to_skip = sorted(processed & raw)
    print("Processing data for dates:", to_process)
    print("Dates already processed: ", to_skip)

    if not to_process:
        print("No new dates to process. Exiting.")
        spark.stop()
        return

    # 2) read and union those dates
    dfs = []
    for d in to_process:
        ds = datetime.fromisoformat(d).strftime("%Y%m%d")
        path = f"{input_prefix}/events_{ds}.parquet"
        dfs.append(spark.read.parquet(path))
    from functools import reduce
    raw_df = reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
        dfs
    )
    print(f"Read {len(to_process)} partitions into DataFrame.")

    # 3) transform
    df1 = base_select_source(raw_df)
    df2 = base_select_renamed(df1)
    print("Applied base transformations.")

    # 4) dedupe
    w = Window.partitionBy(
        "event_date_dt", "stream_id", "user_pseudo_id",
        "session_id", "event_name", "event_timestamp", "_params_json"
    ).orderBy(col("event_timestamp"))
    df3 = (
        df2
        .withColumn("_params_json", to_json(col("params_map")))
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn", "_params_json", "params_map")
    )
    print("Deduplicated events.")

    # 5) write
    event_dates = [row["event_date_dt"] for row in df3.select("event_date_dt").distinct().collect()]
    df3.write.mode("overwrite").partitionBy("event_date_dt").parquet(output_prefix)
    print(f"Wrote transformed partition(s) {event_dates} to {output_prefix}")

    spark.stop()


if __name__ == "__main__":
    # args: optional start_date_str and days
    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(start_date_str=sys.argv[1])
    elif len(sys.argv) == 3:
        main(start_date_str=sys.argv[1], days=int(sys.argv[2]))
    else:
        print("Usage: base_ga4_events.py [start_date YYYY-MM-DD] [days]")
        sys.exit(1)
