"""Rule-based sensor health (spec 10): flatline and spike, per sensor per hour.

ponytail: "out of range" is not duplicated here - DQ05 already quarantines
out-of-range rows at the row grain (config/dq.yaml `ranges`), so the
quarantine table already answers that question. Add a per-sensor rollup here
if the dashboard needs one; nothing in the spec's pages requires it yet.

All 19 sensors are melted into one long (ts, sensor, value) table and windowed
once with `partitionBy("sensor")`. An earlier version built one Window per
sensor with no partitioning and unioned 19 of them - Spark has to shove all
rows through a single task for an unpartitioned window ("No Partition Defined
for Window operation" in the logs), and 19 unioned copies of that produced a
query plan big enough to exhaust driver heap on this project's memory-
constrained dev container. Partitioning by sensor is both the correct fix and
simpler code.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from iop.transform.columns import PROCESS_SENSOR_COLUMNS


def build_sensor_health(typed_df: DataFrame, dq_cfg: dict) -> DataFrame:
    cfg = dq_cfg["sensor_health"]
    min_consecutive = cfg["flatline_min_consecutive"]
    std_threshold = cfg["flatline_std_threshold"]
    z_threshold = cfg["spike_zscore_threshold"]
    roll_window = cfg["spike_rolling_window"]

    hourly = typed_df.withColumn("hour_ts", F.date_trunc("hour", F.col("ts")))

    long_df = hourly.select(
        "ts",
        "hour_ts",
        F.explode(
            F.array(
                *[
                    F.struct(F.lit(sensor).alias("sensor"), F.col(sensor).alias("value"))
                    for sensor in PROCESS_SENSOR_COLUMNS
                ]
            )
        ).alias("s"),
    ).select("ts", "hour_ts", "s.sensor", "s.value")

    w_roll = Window.partitionBy("sensor").orderBy("ts").rowsBetween(-roll_window, 0)
    w_flat = Window.partitionBy("sensor").orderBy("ts").rowsBetween(-(min_consecutive - 1), 0)

    per_row = (
        long_df.withColumn("roll_mean", F.avg("value").over(w_roll))
        .withColumn("roll_std", F.stddev("value").over(w_roll))
        .withColumn("flat_std", F.stddev("value").over(w_flat))
        .withColumn(
            "zscore",
            F.when(
                F.col("roll_std") > 0, (F.col("value") - F.col("roll_mean")) / F.col("roll_std")
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("is_flatline_point", F.col("flat_std") < F.lit(std_threshold))
        .withColumn("is_spike_point", F.abs(F.col("zscore")) > F.lit(z_threshold))
    )

    return (
        per_row.groupBy("hour_ts", "sensor")
        .agg(F.max("is_flatline_point").alias("flatline"), F.max("is_spike_point").alias("spike"))
        .select("hour_ts", "sensor", "flatline", "spike")
    )
