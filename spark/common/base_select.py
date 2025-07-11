# common/base_select.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    to_date, col, lit, lower, trim, regexp_replace,
    struct, when, expr, explode_outer, collect_list, monotonically_increasing_id
)
from pyspark.sql.types import ArrayType, StructType


DUMMY_PROPERTY_ID = 999999999


def base_select_source(df: DataFrame) -> DataFrame:
    return df.select(
        to_date(col("event_date"), "yyyyMMdd").alias("event_date_dt"),
        col("event_timestamp"),
        col("event_name"),
        col("event_params"),
        col("event_previous_timestamp"),
        col("event_value_in_usd"),
        col("event_bundle_sequence_id"),
        col("event_server_timestamp_offset"),
        col("user_id"),
        col("user_pseudo_id"),
        col("privacy_info"),
        col("user_properties"),
        col("user_first_touch_timestamp"),
        col("user_ltv"),
        col("device"),
        col("geo"),
        col("app_info"),
        col("traffic_source"),
        col("stream_id"),
        col("platform"),
        col("ecommerce.total_item_quantity"),
        col("ecommerce.purchase_revenue_in_usd"),
        col("ecommerce.purchase_revenue"),
        col("ecommerce.refund_value_in_usd"),
        col("ecommerce.refund_value"),
        col("ecommerce.shipping_value_in_usd"),
        col("ecommerce.shipping_value"),
        col("ecommerce.tax_value_in_usd"),
        col("ecommerce.tax_value"),
        col("ecommerce.unique_items"),
        col("ecommerce.transaction_id"),
        col("items"),
        lit(DUMMY_PROPERTY_ID).cast("long").alias("property_id")
    )


def normalize_items(df: DataFrame) -> DataFrame:
    """
    Efficiently rebuilds the `items` array-of-structs by:
      1) tagging each row with a unique ID
      2) exploding only the items column
      3) collecting back into an array keyed on that ID
      4) dropping the temp ID and original items
    """
    # If there's no items array or it's not an array of structs, nothing to do
    fld = next((f for f in df.schema.fields if f.name == "items"), None)
    if not fld or not isinstance(fld.dataType, ArrayType):
        return df
    elem = fld.dataType.elementType
    if not isinstance(elem, StructType):
        return df

    # 1) add a stable row id
    df_id = df.withColumn("__row_id", monotonically_increasing_id())

    # 2) explode items into a small two-column DF: (__row_id, item)
    exploded = df_id.select(
        "__row_id",
        explode_outer(col("items")).alias("__item")
    )

    # 3) build a struct of all fields present under __item
    item_fields = [f.name for f in elem.fields]
    struct_cols = [col(f"__item.{n}").alias(n) for n in item_fields]
    exploded_struct = exploded.select(
        "__row_id",
        struct(*struct_cols).alias("item_struct")
    )

    # 4) collect back into an array per __row_id
    collected = exploded_struct.groupBy("__row_id") \
        .agg(collect_list("item_struct").alias("items_clean"))

    # 5) join back to the original DF
    joined = df_id.join(collected, on="__row_id", how="left") \
                  .drop("items", "__row_id") \
                  .withColumnRenamed("items_clean", "items")

    return joined


def base_select_renamed(df: DataFrame) -> DataFrame:
    """
    Flattens and renames GA4 event-level fields using a single params_map
    to avoid expensive shuffles. Assumes df is output of base_select_source().

    Steps:
      1) Build a map of all event_params entries
      2) Extract needed params via direct map access
      3) Drop raw arrays
      4) Flatten nested structs and repack ecommerce

    Note: user dims are staged from the base_select_usr_source() (e.g., user_properties)
    """

    # 0) explode item field
    df = normalize_items(df)

    # 1) Build params_map MapType from event_params; replaced the need for the expensive unnest_key, which triggers shuffle via explode()
    df = df.withColumn(
        "params_map",
        expr("map_from_entries(transform(event_params, x -> struct(x.key, x.value)))") # map_from_entries() efficient Spark SQL explode
    )

    # 2) Extract scalar fields from params_map; only these 11 keys out of 33 unique keys; this is a design choice for downstream
    df = (
        df 
        .withColumn("page_location",        col("params_map.page_location.string_value"))
        .withColumn("session_id",           col("params_map.ga_session_id.int_value"))
        .withColumn("session_number",       col("params_map.ga_session_number.int_value"))
        .withColumn("engagement_time_msec", col("params_map.engagement_time_msec.int_value"))
        .withColumn("page_title",           col("params_map.page_title.string_value"))
        .withColumn("page_referrer",        col("params_map.page_referrer.string_value"))
        .withColumn("event_source",         lower(col("params_map.source.string_value")))
        .withColumn("event_medium",         lower(col("params_map.medium.string_value")))
        .withColumn("event_campaign",       lower(col("params_map.campaign.string_value")))
        .withColumn("event_content",        lower(col("params_map.content.string_value")))
        .withColumn("event_term",           lower(col("params_map.term.string_value")))
        .withColumn(
            "session_engaged",
            expr(
                "coalesce(params_map.session_engaged.int_value, "
                "case when params_map.session_engaged.string_value = '1' then 1 end)"
            )
        )
        # drop raw arrays since we have extracted what we need
        .drop("event_params")
    )

    # 3) Final flatten & rename
    return df.select(
        # date & time
        col("event_date_dt"),
        col("event_timestamp"),
        lower(regexp_replace(trim(col("event_name")), " ", "_")).alias("event_name"),

        # privacy_info
        col("privacy_info.analytics_storage").alias("privacy_info_analytics_storage"),
        col("privacy_info.ads_storage").alias("privacy_info_ads_storage"),
        col("privacy_info.uses_transient_token").alias("privacy_info_uses_transient_token"),

        # user identifiers
        col("user_id"),
        col("user_pseudo_id"),
        col("user_first_touch_timestamp"),

        # user_ltv
        col("user_ltv.revenue").alias("user_ltv_revenue"),
        col("user_ltv.currency").alias("user_ltv_currency"),

        # device
        col("device.category").alias("device_category"),
        col("device.mobile_brand_name").alias("device_mobile_brand_name"),
        col("device.mobile_model_name").alias("device_mobile_model_name"),
        col("device.mobile_marketing_name").alias("device_mobile_marketing_name"),
        col("device.mobile_os_hardware_model").alias("device_mobile_os_hardware_model"),
        col("device.operating_system").alias("device_operating_system"),
        col("device.operating_system_version").alias("device_operating_system_version"),
        col("device.vendor_id").alias("device_vendor_id"),
        col("device.advertising_id").alias("device_advertising_id"),
        col("device.language").alias("device_language"),
        col("device.is_limited_ad_tracking").alias("device_is_limited_ad_tracking"),
        col("device.time_zone_offset_seconds").alias("device_time_zone_offset_seconds"),

        # geo
        col("geo.continent").alias("geo_continent"),
        col("geo.country").alias("geo_country"),
        col("geo.region").alias("geo_region"),
        col("geo.city").alias("geo_city"),
        col("geo.sub_continent").alias("geo_sub_continent"),
        col("geo.metro").alias("geo_metro"),

        # app_info
        col("app_info.id").alias("app_info_id"),
        col("app_info.version").alias("app_info_version"),
        col("app_info.install_store").alias("app_info_install_store"),
        col("app_info.firebase_app_id").alias("app_info_firebase_app_id"),
        col("app_info.install_source").alias("app_info_install_source"),

        # traffic_source (renamed to user_)
        col("traffic_source.name").alias("user_campaign"),
        col("traffic_source.medium").alias("user_medium"),
        col("traffic_source.source").alias("user_source"),

        # stream and platform
        col("stream_id"),
        col("platform"),

        # ecommerce repack
        struct(
            col("total_item_quantity"),
            col("purchase_revenue_in_usd"),
            col("purchase_revenue"),
            col("refund_value_in_usd"),
            col("refund_value"),
            col("shipping_value_in_usd"),
            col("shipping_value"),
            col("tax_value_in_usd"),
            col("tax_value"),
            col("unique_items"),
            col("transaction_id"),
        ).alias("ecommerce"),

        # raw items array
        col("items"),

        # extracted params
        col("session_id"),
        col("page_location"),
        col("session_number"),
        col("engagement_time_msec"),
        col("page_title"),
        col("page_referrer"),
        col("event_source"),
        col("event_medium"),
        col("event_campaign"),
        col("event_content"),
        col("event_term"),
        col("session_engaged"),

        # event flags
        when(col("event_name") == "page_view", lit(1)).otherwise(lit(0)).alias("is_page_view"),
        when(col("event_name") == "purchase", lit(1)).otherwise(lit(0)).alias("is_purchase"),

        # property
        col("property_id"),

        # for dedup logic:
        col("params_map")
    )


def base_select_usr_source(df: DataFrame) -> DataFrame:

    return df.select(
        col("user_info.last_active_timestamp_micros")
            .alias("user_info_last_active_timestamp_micros"),
        col("user_info.user_first_touch_timestamp_micros")
            .alias("user_info_user_first_touch_timestamp_micros"),
        col("user_info.first_purchase_date")
            .alias("user_info_first_purchase_date"),

        col("device.operating_system")
            .alias("device_operating_system"),
        col("device.category").alias("device_category"),
        col("device.mobile_brand_name")
            .alias("device_mobile_brand_name"),
        col("device.mobile_model_name")
            .alias("device_mobile_model_name"),
        col("device.unified_screen_name")
            .alias("device_unified_sceen_name"),

        col("geo.city").alias("geo_city"),
        col("geo.country").alias("geo_country"),
        col("geo.continent").alias("geo_continent"),
        col("geo.region").alias("geo_region"),

        col("user_ltv.revenue_in_usd")
            .alias("user_ltv_revenue_in_usd"),
        col("user_ltv.sessions").alias("user_ltv_sessions"),
        col("user_ltv.engagement_time_millis")
            .alias("user_ltv_engagement_time_millis"),
        col("user_ltv.purchases").alias("user_ltv_purchases"),
        col("user_ltv.engaged_sessions")
            .alias("user_ltv_engaged_sessions"),
        col("user_ltv.session_duration_micros")
            .alias("user_ltv_session_duration_micros"),

        col("predictions.in_app_purchase_score_7d")
            .alias("predictions_in_app_purchase_score_7d"),
        col("predictions.purchase_score_7d")
            .alias("predictions_purchase_score_7d"),
        col("predictions.churn_score_7d")
            .alias("predictions_churn_score_7d"),
        col("predictions.revenue_28d_in_usd")
            .alias("predictions_revenue_28d_in_usd"),

        col("privacy_info.is_limited_ad_tracking")
            .alias("privacy_info_is_limited_ad_tracking"),
        col("privacy_info.is_ads_personalization_allowed")
            .alias("privacy_info_is_ads_personalization_allowed"),

        to_date(col("occurrence_date"), "yyyyMMdd")
            .alias("occurrence_date"),
        to_date(col("last_updated_date"), "yyyyMMdd")
            .alias("last_updated_date"),

        col("user_properties"),
        col("audiences")
    )
