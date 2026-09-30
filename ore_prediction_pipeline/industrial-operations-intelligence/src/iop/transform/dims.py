"""Gold dimensions (spec 7.3). dim_time carries the shift assumption (D02);
dim_sensor's ranges come from config/dq.yaml, never hardcoded."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from iop.quality.rules import expand_ranges
from iop.transform.columns import PROCESS_SENSOR_COLUMNS


def shift_expr(hour_ts_col: str, shift_starts_hour: int):
    """D02: three 8h shifts starting at shift_starts_hour (default 06:00): A/B/C.
    Shared by dim_time and ML's calendar feature - one formula, not two."""
    shift_index = ((F.hour(hour_ts_col) - F.lit(shift_starts_hour) + 24) % 24) / 8
    return F.when(shift_index < 1, "A").when(shift_index < 2, "B").otherwise("C")


def build_dim_time(hours_df: DataFrame, shift_starts_hour: int) -> DataFrame:
    """hours_df: any DataFrame with a distinct `hour_ts` column."""
    shift = shift_expr("hour_ts", shift_starts_hour)
    return hours_df.select("hour_ts").distinct().select(
        F.col("hour_ts").alias("hour_key"),
        F.col("hour_ts").alias("ts"),
        F.to_date("hour_ts").alias("date"),
        F.hour("hour_ts").alias("hour"),
        shift.alias("shift"),
        F.dayofweek("hour_ts").alias("day_of_week"),
        F.weekofyear("hour_ts").alias("week"),
        F.month("hour_ts").alias("month"),
    )


def build_dim_sensor(spark: SparkSession, dq_cfg: dict) -> DataFrame:
    ranges = expand_ranges(dq_cfg["ranges"])

    def group_of(name: str) -> str:
        if name.startswith("col"):
            return "column"
        if name in ("starch_flow", "amina_flow"):
            return "reagent"
        return "pulp"

    rows = [
        (name, name, "unit_unknown", group_of(name), ranges.get(name, (None, None))[0], ranges.get(name, (None, None))[1])
        for name in PROCESS_SENSOR_COLUMNS
    ]
    return spark.createDataFrame(
        rows, ["sensor_key", "name", "unit", "group", "valid_min", "valid_max"]
    )


def build_dim_plant(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [("plant_1", "Flotation Plant 1", False)],
        ["plant_key", "name", "is_synthetic"],
    )
