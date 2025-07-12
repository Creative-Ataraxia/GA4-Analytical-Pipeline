# marts/dim_ga4_sessions.py
"""
Spark job to build the GA4 sessions dimension (dim_ga4_sessions):
* Reads staging events and session traffic sources (and optionally derived session properties)
* Selects the first non-`first_visit`/`session_start` event per session
* Projects session start attributes and computes `is_first_session`
* Joins traffic source and optional derived properties
* Writes out partitioned Parquet by session_start_date to MinIO
"""

import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, row_number
)
from pyspark.sql.window import Window


def main(start_date_str=None, days=None, join_derived=False):
    # Paths
    events_path = "s3a://ga4-bronze/stg_ga4_events"
    traffic_path = "s3a://ga4-bronze/stg_ga4_sessions_traffic_sources"
    derived_path = "s3a://ga4-bronze/stg_ga4_derived_session_properties"
    output_path = "s3a://ga4-bronze/dim_ga4_sessions"

    # Spark session
    spark = (
        SparkSession.builder
            .appName("ga4_dim_sessions_job")
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

    # Read staging events
    df_events = spark.read.parquet(events_path)

    # Optional date filter on event_date_dt
    if start_date_str:
        start = datetime.fromisoformat(start_date_str).date()
        if days:
            end = start + timedelta(days=days-1)
            df_events = df_events.filter(
                (col("event_date_dt") >= lit(start)) &
                (col("event_date_dt") <= lit(end))
            )
        else:
            df_events = df_events.filter(col("event_date_dt") == lit(start))

    # 1) Filter out session boundary events
    df_filtered = df_events.filter(
        (col("event_name") != "first_visit") &
        (col("event_name") != "session_start")
    )

    # 2) Pick the first event per session_key
    w = Window.partitionBy("session_key").orderBy(col("event_timestamp"))
    df_first = df_filtered.withColumn("rn", row_number().over(w))
    df_first = df_first.filter(col("rn") == 1).drop("rn")

    # 3) Project session start dims
    df_sess = df_first.select(
        col("session_key"),
        col("event_date_dt").alias("session_start_date"),
        col("event_timestamp").alias("session_start_timestamp"),
        col("page_path").alias("landing_page_path"),
        col("page_location").alias("landing_page"),
        col("page_hostname").alias("landing_page_hostname"),
        col("page_referrer").alias("landing_page_referrer"),

        col("geo_continent"), col("geo_country"), col("geo_region"), col("geo_city"),
        col("geo_sub_continent"), col("geo_metro"),
        col("stream_id"), col("platform"),

        col("device_category"), col("device_mobile_brand_name"), col("device_mobile_model_name"),
        col("device_mobile_marketing_name"), col("device_mobile_os_hardware_model"),
        col("device_operating_system"), col("device_operating_system_version"),
        col("device_vendor_id"), col("device_advertising_id"), col("device_language"),
        col("device_is_limited_ad_tracking"), col("device_time_zone_offset_seconds"),
        col("device_browser"), col("device_web_info_browser"), col("device_web_info_browser_version"),
        col("device_web_info_hostname"),

        col("session_number"),
        (col("session_number") == lit(1)).alias("is_first_session"),

        col("user_campaign"), col("user_medium"), col("user_source")
    )

    # 4) Join traffic sources
    df_traffic = spark.read.parquet(traffic_path)
    df_joined = df_sess.join(
        df_traffic, on="session_key", how="left"
    )

    # 5) Optional join derived properties
    if join_derived:
        df_derived = spark.read.parquet(derived_path)
        df_joined = df_joined.join(
            df_derived, on="session_key", how="left"
        )

    # 6) Write out
    df_joined.write.mode("overwrite").partitionBy("session_start_date").parquet(output_path)
    dates = [r.session_start_date for r in df_joined.select("session_start_date").distinct().collect()]
    print(f"Wrote session dims to {output_path}, dates: {dates}")

    spark.stop()


if __name__ == "__main__":
    # Usage: dim_ga4_sessions.py [start_date YYYY-MM-DD] [days] [join_derived true|false]
    join_flag = False
    if len(sys.argv) == 4:
        join_flag = sys.argv[3].lower() == 'true'
    if len(sys.argv) >= 3:
        main(start_date_str=sys.argv[1], days=int(sys.argv[2]), join_derived=join_flag)
    elif len(sys.argv) == 2:
        main(start_date_str=sys.argv[1], join_derived=join_flag)
    else:
        main()
