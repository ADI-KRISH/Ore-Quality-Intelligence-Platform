"""fact_process_hourly and fact_lab_quality (spec 7.3)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from iop.transform.columns import PROCESS_SENSOR_COLUMNS


def build_fact_process_hourly(typed_df: DataFrame, dq_cfg: dict) -> DataFrame:
    # last15min_mean and slope are deliberately NOT computed: `ts` is the raw
    # CSV's own timestamp, which is hour-resolution only (spec 4.1's own
    # confirmed trap - all ~180 rows in an hour share one identical value).
    # F.minute("ts") is therefore always 0 and _secs (ts - hour_ts) is always
    # 0, so a "last 15 minutes" or "within-hour slope" feature would be
    # either silently all-null (confirmed: this is what shipped first) or
    # would require inventing a within-hour row order the source data does
    # not actually contain - exactly what spec 4.1 says not to do.
    expected_rows = dq_cfg["hour_completeness"]["expected_rows_per_hour"]
    hourly = typed_df.withColumn("hour_ts", F.date_trunc("hour", F.col("ts")))

    aggs = [F.count(F.lit(1)).alias("row_count")]
    for sensor in PROCESS_SENSOR_COLUMNS:
        v = F.col(sensor)
        aggs += [
            F.avg(v).alias(f"{sensor}_mean"),
            F.stddev(v).alias(f"{sensor}_std"),
            F.min(v).alias(f"{sensor}_min"),
            F.max(v).alias(f"{sensor}_max"),
        ]

    result = hourly.groupBy("hour_ts").agg(*aggs)
    return result.withColumn(
        "completeness_pct", (F.col("row_count") / F.lit(expected_rows) * 100).cast("double")
    )


def compute_off_spec_threshold(lab_results: DataFrame, train_start: str, train_end: str, percentile: int) -> float:
    """D04: off-spec = above this percentile of TRAINING silica only (never
    test/val, and never an hour flagged lab_is_interpolated - D06)."""
    training = lab_results.filter(
        (F.col("hour_ts") >= train_start)
        & (F.col("hour_ts") <= train_end)
        & (~F.col("lab_is_interpolated"))
    )
    row = training.selectExpr(f"percentile_approx(pct_silica_concentrate, {percentile / 100}) as p").first()
    return float(row["p"])


def build_fact_lab_quality(lab_results: DataFrame, off_spec_threshold: float) -> DataFrame:
    return lab_results.withColumn(
        "is_off_spec", F.col("pct_silica_concentrate") > F.lit(off_spec_threshold)
    )
