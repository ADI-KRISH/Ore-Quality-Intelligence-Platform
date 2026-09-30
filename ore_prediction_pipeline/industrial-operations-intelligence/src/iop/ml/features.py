"""Feature builder (spec 9.1-9.2). Pure function: three Gold/Silver
DataFrames in, one feature-matrix DataFrame out - keeps tests/test_leakage.py
able to exercise it on tiny synthetic data instead of a real pipeline run.

Nowcast framing: predict hour h + horizon's lab silica using only process
data through hour h and lab data through h - L (the lab delay). Every join
below shifts the SOURCE row's key to align with h, never the other way -
that's what keeps "backwards" (future) joins structurally impossible rather
than just policed by convention.

pct_iron_concentrate never appears here (CLAUDE.md) - only ml/ablation.py is
allowed to add it back in, later, as a deliberate, reported exception.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from iop.config import MlConfig
from iop.transform.columns import PROCESS_SENSOR_COLUMNS
from iop.transform.dims import shift_expr

SENSOR_STATS = ("mean", "std", "min", "max")

_DEFAULT_FEATURES_CFG = {"sensor_lag_hours": [1, 2, 3], "lab_silica_lag_hours": [1, 2, 3]}


def load_features_config(path: str | Path = "config/features.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def _shift_hours(col: str, hours: int):
    return F.col(col) + F.expr(f"INTERVAL {hours} HOURS")


def build_features(
    fact_process_hourly: DataFrame,
    feed_quality: DataFrame,
    fact_lab_quality: DataFrame,
    ml_cfg: MlConfig,
    horizon: int = 0,
    min_completeness_pct: float | None = None,
    features_cfg: dict | None = None,
) -> DataFrame:
    features_cfg = features_cfg or _DEFAULT_FEATURES_CFG
    sensor_lag_hours = features_cfg["sensor_lag_hours"]
    lab_silica_lag_hours = features_cfg["lab_silica_lag_hours"]
    lab_delay = ml_cfg.lab_delay_hours

    current_cols = [f"{s}_{stat}" for s in PROCESS_SENSOR_COLUMNS for stat in SENSOR_STATS]
    base = fact_process_hourly.select("hour_ts", "plant_key", "completeness_pct", *current_cols)

    for lag in sensor_lag_hours:
        lag_source = fact_process_hourly.select(
            _shift_hours("hour_ts", lag).alias("hour_ts"),
            "plant_key",
            *[F.col(f"{s}_mean").alias(f"{s}_mean_lag{lag}") for s in PROCESS_SENSOR_COLUMNS],
        )
        base = base.join(lag_source, ["hour_ts", "plant_key"], "left")

    feed_source = feed_quality.select("hour_ts", "pct_iron_feed", "pct_silica_feed")
    base = base.join(feed_source, "hour_ts", "left")

    for lag in lab_silica_lag_hours:
        hours_back = lab_delay + (lag - 1)
        lab_lag_source = fact_lab_quality.select(
            _shift_hours("hour_ts", hours_back).alias("hour_ts"),
            "plant_key",
            F.col("pct_silica_concentrate").alias(f"lab_silica_lag{lag}"),
            F.col("lab_is_interpolated").alias(f"_lab_interp_lag{lag}"),
        )
        base = base.join(lab_lag_source, ["hour_ts", "plant_key"], "left")
        # D06: a lag that looks back at an interpolated hour is untrustworthy -
        # null it out rather than pass the value through.
        base = base.withColumn(
            f"lab_silica_lag{lag}",
            F.when(~F.coalesce(F.col(f"_lab_interp_lag{lag}"), F.lit(False)), F.col(f"lab_silica_lag{lag}")),
        ).drop(f"_lab_interp_lag{lag}")

    base = base.withColumn("hour_of_day", F.hour("hour_ts")).withColumn(
        "shift", shift_expr("hour_ts", ml_cfg.shift_starts_hour)
    )

    if min_completeness_pct is not None:
        base = base.filter(F.col("completeness_pct") >= min_completeness_pct)
    base = base.drop("completeness_pct")

    # Target: hour h's features paired with the label at h + horizon. D06:
    # an interpolated target hour is dropped entirely (inner join + filter),
    # not just nulled - it must never be used as ground truth.
    target_source = fact_lab_quality.select(
        _shift_hours("hour_ts", -horizon).alias("hour_ts"),
        "plant_key",
        F.col("pct_silica_concentrate").alias("target"),
        F.col("lab_is_interpolated").alias("_target_interp"),
    )
    result = base.join(target_source, ["hour_ts", "plant_key"], "inner").filter(~F.col("_target_interp"))
    return result.drop("_target_interp")
