"""fact_process_hourly and fact_lab_quality (spec 7.3)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from iop.transform.columns import PROCESS_SENSOR_COLUMNS


def build_fact_process_hourly(typed_df: DataFrame, dq_cfg: dict) -> DataFrame:
    expected_rows = dq_cfg["hour_completeness"]["expected_rows_per_hour"]
    hourly = typed_df.withColumn("hour_ts", F.date_trunc("hour", F.col("ts"))).withColumn(
        "_secs", F.unix_timestamp("ts") - F.unix_timestamp("hour_ts")
    )

    aggs = [F.count(F.lit(1)).alias("row_count")]
    for sensor in PROCESS_SENSOR_COLUMNS:
        v = F.col(sensor)
        aggs += [
            F.avg(v).alias(f"{sensor}_mean"),
            F.stddev(v).alias(f"{sensor}_std"),
            F.min(v).alias(f"{sensor}_min"),
            F.max(v).alias(f"{sensor}_max"),
            F.avg(F.when(F.minute("ts") >= 45, v)).alias(f"{sensor}_last15min_mean"),
            # Spark returns NULL for division by zero/null (non-ANSI, the
            # default), so a var_samp==0 guard is unnecessary - and harmful:
            # referencing var_samp("_secs") twice per sensor inside a
            # CaseWhen, repeated across 19 sensors in one wide aggregate, is
            # exactly the shape that makes Catalyst's expression
            # canonicalization (CommutativeExpression.gatherCommutative)
            # blow up and exhaust driver heap.
            (F.covar_samp("_secs", v) / F.var_samp("_secs")).alias(f"{sensor}_slope"),
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
