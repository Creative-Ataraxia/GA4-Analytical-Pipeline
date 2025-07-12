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
    col, expr, when, regexp_replace, lit, url_encode,
    md5, concat_ws, unhex, base64, to_json, parse_url
)
from common.ga4_spark_utils import list_processed_dates, list_raw_dates

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
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") # Spark will only replace the partitions present in this DataFrame. 
            .getOrCreate()
    )

    # Read base events
    df = spark.read.parquet(input_prefix)

    # Optional date filtering
    if start_date_str:
        start = datetime.fromisoformat(start_date_str).date()
        if days:
            end = start + timedelta(days=days-1)
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
            to_json(col("event_params"))
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
        .withColumn("original_page_location", col("page_location")) # keep originals
        .withColumn("original_page_location", url_encode(col("original_page_location"))) # percent-encode unsafe chars
        .withColumn("page_location", regexp_replace(col("page_location"), "\\?.*$", "")) # strip off query string
        .withColumn("original_page_referrer", col("page_referrer"))
        .withColumn("original_page_referrer", url_encode(col("original_page_referrer"))) 
        .withColumn("page_referrer", regexp_replace(col("page_referrer"), "\\?.*$", ""))
    )

    # 7) enrich URL fields: path, hostname, query string
    df = (
        df
        .withColumn("page_path",     parse_url(col("original_page_location"), lit("PATH")))
        .withColumn("page_hostname", parse_url(col("original_page_location"), lit("HOST")))
        .withColumn("page_query_string", parse_url(col("original_page_location"), lit("QUERY")))
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

    all_cols = df.columns   # whatever survived all upstream steps

    # the staging columns
    staging_cols = [
        "client_key", "session_key", "session_partition_key", "event_key",
        "original_page_location", "original_page_referrer",
        "page_path", "page_hostname", "page_query_string",
        "page_key", "page_engagement_key"
    ]

    # keep only those staging_cols that really exist (they should!)
    staging_cols = [c for c in staging_cols if c in all_cols]

    # now build the final column order:
    # start with everything that wasn't a staging col
    # then append the staging cols in desired order
    base_cols = [c for c in all_cols if c not in staging_cols]

    # force event_date_dt to be first
    if "event_date_dt" in base_cols:
        base_cols.remove("event_date_dt")
        base_cols = ["event_date_dt"] + base_cols

    final_cols = base_cols + staging_cols

    # and select in that order
    df = df.select(*final_cols)

    # Write output
    df = df.withColumn("_event_date_dt", col("event_date_dt")) # spark by default drop the column used to partitionBy
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
