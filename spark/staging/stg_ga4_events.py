# staging/stg_ga4_events.py

"""
Spark job to implement the GA4 staging events model:
* Reads base events from MinIO (s3a://ga4-bronze/base_ga4_events)
* Adds client, session, session-partition, and event surrogate keys (base64-md5)
* Detects & overrides GCLID attribution fields
* Strips query parameters, preserves originals, extracts page path, hostname, query string
* Adds page_key and page_engagement_key
* Writes out partitioned Parquet by event_date_dt to s3a://ga4-bronze/stg_ga4_events
"""

import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, when, regexp_replace, lit,
    md5, concat_ws, unhex, base64, to_json, parse_url
)


def main(start_date_str=None, days=None):
    # Input/Output prefixes
    input_prefix = "s3a://ga4-bronze/base_ga4_events"
    output_prefix = "s3a://ga4-bronze/stg_ga4_events"

    # Initialize Spark
    spark = (
        SparkSession.builder
            .appName("ga4_stg_events_job")
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

    # Read base events
    df = spark.read.parquet(input_prefix)

    # Optional date filtering
    if start_date_str:
        start = datetime.fromisoformat(start_date_str).date()
        if days:
            end = start + timedelta(days=days)
            df = df.filter(
                (col("event_date_dt") >= lit(start)) &
                (col("event_date_dt") <= lit(end))
            )
        else:
            df = df.filter(col("event_date_dt") >= lit(start))

    # 1) client_key = base64(unhex(md5(user_pseudo_id||stream_id)))
    df = df.withColumn(
        "client_key",
        base64(unhex(md5(concat_ws("", col("user_pseudo_id"), col("stream_id")))))
    )

    # 2) session_key = base64(unhex(md5(client_key||session_id)))
    df = df.withColumn(
        "session_key",
        base64(unhex(md5(concat_ws("", col("client_key"), col("session_id").cast("string")))))
    )

    # 3) session_partition_key = session_key || event_date_dt
    df = df.withColumn(
        "session_partition_key",
        concat_ws("", col("session_key"), col("event_date_dt").cast("string"))
    )

    # 4) event_key = base64(unhex(md5(concat(... to_json(params_map) ...)))); originally event_params, renamed in base_select.py
    df = df.withColumn(
        "event_key",
        base64(unhex(md5(concat_ws("", 
            col("client_key"),
            col("session_id").cast("string"),
            col("event_name"),
            col("event_timestamp").cast("string"),
            to_json(col("params_map"))
        ))))
    )

    # 5) detect_gclid override for source/medium/campaign
    df = df.withColumn(
        "event_source",
        when(
            col("page_location").contains("gclid") & col("event_source").isNull(),
            lit("google")
        ).otherwise(col("event_source"))
    ).withColumn(
        "event_medium",
        when(
            col("page_location").contains("gclid") & (
                col("event_medium").isNull() |
                (col("event_medium") == "organic")
            ),
            lit("cpc")
        ).otherwise(col("event_medium"))
    ).withColumn(
        "event_campaign",
        when(
            col("page_location").contains("gclid") & (
                col("event_campaign").isNull() |
                col("event_campaign").isin("organic", "(organic)")
            ),
            lit("(cpc)")
        ).otherwise(col("event_campaign"))
    )

    # 6) strip query params, keep originals
    df = (
        df
        .withColumn("original_page_location", col("page_location"))
        .withColumn("page_location", regexp_replace(col("page_location"), "\\?.*$", ""))
        .withColumn("original_page_referrer", col("page_referrer"))
        .withColumn("page_referrer", regexp_replace(col("page_referrer"), "\\?.*$", ""))
    )

    # 7) enrich URL fields: path, hostname, query string
    df = (
        df
        .withColumn("page_path",     parse_url(col("original_page_location"), "PATH"))
        .withColumn("page_hostname", parse_url(col("original_page_location"), "HOST"))
        .withColumn("page_query_string", parse_url(col("original_page_location"), "QUERY"))
    )

    # 8) page_key and page_engagement_key
    df = (
        df
        .withColumn(
            "page_key",
            concat_ws("", col("event_date_dt").cast("string"), col("page_location"))
        )
        .withColumn(
            "page_engagement_key",
            when(
                col("event_name") == "page_view",
                base64(unhex(md5(concat_ws("", col("session_key"), col("page_referrer")))))
            ).otherwise(
                base64(unhex(md5(concat_ws("", col("session_key"), col("page_location")))))
            )
        )
    )

    # Final select to lock in output schema; for schema validation at-a-glance
    final_cols = [
        # base_ga4__events columns
        "event_date_dt","event_timestamp","event_name",
        "privacy_info_analytics_storage","privacy_info_ads_storage","privacy_info_uses_transient_token",
        "user_id","user_pseudo_id","user_first_touch_timestamp",
        "user_ltv_revenue","user_ltv_currency",
        "device_category","device_mobile_brand_name","device_mobile_model_name",
        "device_mobile_marketing_name","device_mobile_os_hardware_model",
        "device_operating_system","device_operating_system_version",
        "device_vendor_id","device_advertising_id","device_language",
        "device_is_limited_ad_tracking","device_time_zone_offset_seconds",
        "device_browser","device_browser_version",
        "device_web_info_browser","device_web_info_browser_version",
        "device_web_info_hostname",
        "geo_continent","geo_country","geo_region","geo_city",
        "geo_sub_continent","geo_metro",
        "app_info_id","app_info_version","app_info_install_store",
        "app_info_firebase_app_id","app_info_install_source",
        "user_campaign","user_medium","user_source",
        "stream_id","platform","ecommerce","items",
        "session_id","page_location","session_number","engagement_time_msec",
        "page_title","page_referrer","event_source","event_medium",
        "event_campaign","event_content","event_term","session_engaged",
        "is_page_view","is_purchase","property_id",
        # staging additions
        "client_key","session_key","session_partition_key","event_key",
        "original_page_location","original_page_referrer","page_path",
        "page_location","page_referrer","page_hostname","page_query_string",
        "page_key","page_engagement_key"
    ]
    df = df.select(*[col(c) for c in final_cols])

    # Write output
    df.write.mode("overwrite").partitionBy("event_date_dt").parquet(output_prefix)
    dates = [row.event_date_dt for row in df.select("event_date_dt").distinct().collect()]
    print(f"Wrote staging data to {output_prefix}, partitions: {dates}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        main(start_date_str=sys.argv[1])
    elif len(sys.argv) == 3:
        main(start_date_str=sys.argv[1], days=int(sys.argv[2]))
    else:
        print("Usage: stg_ga4_events.py [start_date YYYY-MM-DD] [days]")
        sys.exit(1)
