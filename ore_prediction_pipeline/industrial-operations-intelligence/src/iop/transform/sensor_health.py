"""Rule-based sensor health (spec 10): flatline and spike, per sensor per hour.

ponytail: "out of range" is not duplicated here - DQ05 already quarantines
out-of-range rows at the row grain (config/dq.yaml `ranges`), so the
quarantine table already answers that question. Add a per-sensor rollup here
if the dashboard needs one; nothing in the spec's pages requires it yet.
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

    frames = []
    for sensor in PROCESS_SENSOR_COLUMNS:
        w_roll = Window.orderBy("ts").rowsBetween(-roll_window, 0)
        w_flat = Window.orderBy("ts").rowsBetween(-(min_consecutive - 1), 0)

        per_row = (
            hourly.select("hour_ts", "ts", F.col(sensor).alias("value"))
            .withColumn("roll_mean", F.avg("value").over(w_roll))
            .withColumn("roll_std", F.stddev("value").over(w_roll))
            .withColumn("flat_std", F.stddev("value").over(w_flat))
            .withColumn(
                "zscore",
                F.when(F.col("roll_std") > 0, (F.col("value") - F.col("roll_mean")) / F.col("roll_std"))
                .otherwise(F.lit(0.0)),
            )
            .withColumn("is_flatline_point", F.col("flat_std") < F.lit(std_threshold))
            .withColumn("is_spike_point", F.abs(F.col("zscore")) > F.lit(z_threshold))
        )
        frames.append(
            per_row.groupBy("hour_ts")
            .agg(
                F.max("is_flatline_point").alias("flatline"),
                F.max("is_spike_point").alias("spike"),
            )
            .withColumn("sensor", F.lit(sensor))
        )

    result = frames[0]
    for f in frames[1:]:
        result = result.unionByName(f)
    return result.select("hour_ts", "sensor", "flatline", "spike")
