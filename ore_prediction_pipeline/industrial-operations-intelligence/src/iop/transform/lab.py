"""silver.lab_results and silver.feed_quality (spec 7.2), per hour.

D06 (docs/decisions.md): when a lab column's raw value differs across more
than `lab_value_stability.max_distinct_values_per_hour` rows in the hour, that
value is not a single hourly assay - it looks interpolated toward the next
one, which would leak the future if used as-is. Those hours are kept (mean
value, for display only) but flagged `lab_is_interpolated = true` (DQ10) so
ML can exclude them from targets, evaluation and lag features.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_lab_results(typed_df: DataFrame, dq_cfg: dict) -> DataFrame:
    threshold = dq_cfg["lab_value_stability"]["max_distinct_values_per_hour"]
    hourly = typed_df.withColumn("hour_ts", F.date_trunc("hour", F.col("ts")))
    return (
        hourly.groupBy("hour_ts")
        .agg(
            F.avg("pct_iron_concentrate").alias("pct_iron_concentrate"),
            F.avg("pct_silica_concentrate").alias("pct_silica_concentrate"),
            F.countDistinct("pct_iron_concentrate").alias("_distinct_iron"),
            F.countDistinct("pct_silica_concentrate").alias("_distinct_silica"),
        )
        .withColumn(
            "lab_is_interpolated",
            (F.col("_distinct_iron") > F.lit(threshold))
            | (F.col("_distinct_silica") > F.lit(threshold)),
        )
        .drop("_distinct_iron", "_distinct_silica")
    )


def build_feed_quality(typed_df: DataFrame) -> DataFrame:
    hourly = typed_df.withColumn("hour_ts", F.date_trunc("hour", F.col("ts")))
    return hourly.groupBy("hour_ts").agg(
        F.avg("pct_iron_feed").alias("pct_iron_feed"),
        F.avg("pct_silica_feed").alias("pct_silica_feed"),
    )
