# common/unnest_key.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer, monotonically_increasing_id, lower
from typing import Optional

def unnest_key(
    df: DataFrame,
    column: str,
    key: str,
    value_type: str = "string_value",
    alias: Optional[str] = None
) -> DataFrame:
    """
    Extracts a key's value from an array-of-structs column like event_params,
    preserving all rows (NULL if key missing).

    Args:
        df: Input DataFrame
        column: Name of the array column (e.g., 'event_params')
        key: The 'key' to extract (e.g., 'ga_session_id')
        value_type: One of 'string_value', 'int_value', etc., or 'lower_string_value'
        alias: Output column name; defaults to the key name.

    Returns:
        DataFrame with the new column appended.
    """
    output_col = alias or key

    # 1) add a stable row id
    df_id = df.withColumn("__row_id", monotonically_increasing_id())

    # 2) explode_outer so rows with no entries still appear
    exploded = df_id.select(
        "__row_id",
        explode_outer(col(column)).alias("__param")
    )

    # 3) pick the right value field
    if value_type == "lower_string_value":
        val = lower(col("__param.value.string_value"))
    else:
        val = col(f"__param.value.{value_type}")

    extracted = (
        exploded
        .filter(col("__param.key") == key)
        .select("__row_id", val.alias(output_col))
    )

    # 4) left join back so we don’t lose rows that lack this key
    joined = df_id.join(extracted, on="__row_id", how="left").drop("__row_id")

    return joined
