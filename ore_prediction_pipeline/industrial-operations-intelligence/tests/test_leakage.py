"""Leakage tests for the ML feature builder (spec 9.2, CLAUDE.md, D06).
Written before iop/ml/features.py exists (PLAYBOOK Days 9-11): the leaky-
construction test below needs no import from that module at all, so it can
prove the failure mode is real and catchable before the real implementation
is written. The rest of this file exercises the real `build_features` once
it exists, checking the same properties on real output.

Ten synthetic hours h0..h9, one hour apart, with `{sensor}_mean` and
`pct_silica_concentrate` both set to the hour's own index (h3's mean is
30.0, its silica is 3.0) - every leak check below is then just "does this
feature's value match its OWN hour's index, or some other hour's?"
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from iop.config import load_config
from iop.transform.columns import PROCESS_SENSOR_COLUMNS

N_HOURS = 10
BASE_HOUR = datetime(2017, 5, 1, 0, 0, 0)
SENSOR = PROCESS_SENSOR_COLUMNS[0]


def _hour_ts(i: int) -> datetime:
    return BASE_HOUR + timedelta(hours=i)


def _ml_cfg():
    return load_config("config/local.yaml").ml  # lab_delay_hours=1, shift_starts_hour=6


@pytest.fixture(scope="module")
def synthetic_gold(spark):
    def build(interpolated_hours: set[int] = frozenset()):
        process_rows = []
        lab_rows = []
        feed_rows = []
        for i in range(N_HOURS):
            ts = _hour_ts(i)
            stats = {}
            for sensor in PROCESS_SENSOR_COLUMNS:
                stats[f"{sensor}_mean"] = float(i * 10)
                stats[f"{sensor}_std"] = 0.0
                stats[f"{sensor}_min"] = 0.0
                stats[f"{sensor}_max"] = 0.0
            process_rows.append(
                {
                    "hour_ts": ts,
                    "plant_key": "plant_1",
                    "row_count": 180,
                    "completeness_pct": 100.0,
                    **stats,
                }
            )
            lab_rows.append(
                {
                    "hour_ts": ts,
                    "plant_key": "plant_1",
                    "pct_iron_concentrate": float(i),
                    "pct_silica_concentrate": float(i),
                    "lab_is_interpolated": i in interpolated_hours,
                }
            )
            feed_rows.append(
                {"hour_ts": ts, "pct_iron_feed": float(i), "pct_silica_feed": float(i)}
            )

        process_cols = ["hour_ts", "plant_key", "row_count", "completeness_pct"] + [
            f"{s}_{stat}" for s in PROCESS_SENSOR_COLUMNS for stat in ("mean", "std", "min", "max")
        ]
        fact_process_hourly = spark.createDataFrame(process_rows).select(*process_cols)
        fact_lab_quality = spark.createDataFrame(lab_rows)
        feed_quality = spark.createDataFrame(feed_rows)
        return fact_process_hourly, feed_quality, fact_lab_quality

    return build


# --- Part 1: prove the check itself has teeth, with no dependency on the
# real implementation (it doesn't exist yet at this point in the workflow). ---


def test_a_leaky_lag_that_uses_the_current_hour_is_caught(synthetic_gold):
    """The correct lag-1 lab feature at hour h5 must be h4's silica (4.0) -
    that's h - L for L=1. A leaky implementation that forgets the delay and
    joins the CURRENT hour's own lab value as "lag1" instead would put 5.0
    there - h5's own value, one hour later than it should be. Build exactly
    that mistake and confirm it produces the forbidden value, proving this
    check would catch it in a real implementation."""
    _, _, fact_lab_quality = synthetic_gold()

    leaky_lag1 = fact_lab_quality.select(
        F.col("hour_ts"),
        F.col("plant_key"),
        F.col("pct_silica_concentrate").alias("lab_silica_lag1"),  # BUG: no shift at all
    )
    row = leaky_lag1.filter(F.col("hour_ts") == _hour_ts(5)).first()

    assert row["lab_silica_lag1"] == 5.0  # h5's own value leaked in
    assert row["lab_silica_lag1"] != 4.0  # h - L (the correct answer) was NOT used


# --- Part 2: the real build_features, once implemented, must not do this. ---


def test_current_hour_process_features_match_their_own_hour(synthetic_gold):
    from iop.ml.features import build_features

    fact_process_hourly, feed_quality, fact_lab_quality = synthetic_gold()
    features = build_features(fact_process_hourly, feed_quality, fact_lab_quality, ml_cfg=_ml_cfg())

    row = features.filter(F.col("hour_ts") == _hour_ts(5)).first()
    assert row[f"{SENSOR}_mean"] == 50.0  # h5's own value
    assert row[f"{SENSOR}_mean"] != 60.0  # not h6's (would mean future data leaked in)


def test_lag_features_use_past_hours_not_present_or_future(synthetic_gold):
    from iop.ml.features import build_features

    fact_process_hourly, feed_quality, fact_lab_quality = synthetic_gold()
    features = build_features(fact_process_hourly, feed_quality, fact_lab_quality, ml_cfg=_ml_cfg())

    row = features.filter(F.col("hour_ts") == _hour_ts(5)).first()
    assert row[f"{SENSOR}_mean_lag1"] == 40.0  # h4 (h - 1)
    assert row[f"{SENSOR}_mean_lag2"] == 30.0  # h3 (h - 2)
    assert row[f"{SENSOR}_mean_lag3"] == 20.0  # h2 (h - 3)


def test_lab_silica_lag_respects_the_lab_delay(synthetic_gold):
    """L=1 (config/local.yaml): at h5, lag1 must be h4 (h - L), never h5 itself."""
    from iop.ml.features import build_features

    fact_process_hourly, feed_quality, fact_lab_quality = synthetic_gold()
    features = build_features(fact_process_hourly, feed_quality, fact_lab_quality, ml_cfg=_ml_cfg())

    row = features.filter(F.col("hour_ts") == _hour_ts(5)).first()
    assert row["lab_silica_lag1"] == 4.0  # h - L
    assert row["lab_silica_lag2"] == 3.0  # h - L - 1
    assert row["lab_silica_lag3"] == 2.0  # h - L - 2
    assert row["lab_silica_lag1"] != 5.0  # never the current hour's own value


def test_pct_iron_concentrate_is_never_a_feature(synthetic_gold):
    from iop.ml.features import build_features

    fact_process_hourly, feed_quality, fact_lab_quality = synthetic_gold()
    features = build_features(fact_process_hourly, feed_quality, fact_lab_quality, ml_cfg=_ml_cfg())

    assert "pct_iron_concentrate" not in features.columns
    assert not any("iron_concentrate" in c for c in features.columns)


def test_horizon_shifts_the_target_not_the_features(synthetic_gold):
    """For horizon k, the target moves to h+k but every feature at h stays
    exactly as it was for k=0 - the model still only knows what happened up
    to h, it's just being asked about a later hour."""
    from iop.ml.features import build_features

    fact_process_hourly, feed_quality, fact_lab_quality = synthetic_gold()
    f0 = build_features(
        fact_process_hourly, feed_quality, fact_lab_quality, ml_cfg=_ml_cfg(), horizon=0
    )
    f2 = build_features(
        fact_process_hourly, feed_quality, fact_lab_quality, ml_cfg=_ml_cfg(), horizon=2
    )

    row0 = f0.filter(F.col("hour_ts") == _hour_ts(3)).first()
    row2 = f2.filter(F.col("hour_ts") == _hour_ts(3)).first()

    assert row0[f"{SENSOR}_mean"] == row2[f"{SENSOR}_mean"]
    assert row0["lab_silica_lag1"] == row2["lab_silica_lag1"]
    assert row0["target"] == 3.0  # silica at h3 (k=0)
    assert row2["target"] == 5.0  # silica at h3+2=h5 (k=2)


def test_interpolated_target_hour_is_excluded(synthetic_gold):
    """D06: an hour whose lab value looks interpolated is not a trustworthy
    label - it must not appear as a target row at all."""
    from iop.ml.features import build_features

    fact_process_hourly, feed_quality, fact_lab_quality = synthetic_gold(interpolated_hours={4})
    features = build_features(fact_process_hourly, feed_quality, fact_lab_quality, ml_cfg=_ml_cfg())

    assert features.filter(F.col("hour_ts") == _hour_ts(4)).count() == 0


def test_lag_referencing_an_interpolated_hour_is_null_not_leaked(synthetic_gold):
    """D06: h5's lag1 looks back to h4 (h - L). If h4 is interpolated, that
    lag feature must be null, never the untrustworthy value itself."""
    from iop.ml.features import build_features

    fact_process_hourly, feed_quality, fact_lab_quality = synthetic_gold(interpolated_hours={4})
    features = build_features(fact_process_hourly, feed_quality, fact_lab_quality, ml_cfg=_ml_cfg())

    row = features.filter(F.col("hour_ts") == _hour_ts(5)).first()
    assert row["lab_silica_lag1"] is None
    assert row["lab_silica_lag2"] == 3.0  # h3, untouched
    assert row["lab_silica_lag3"] == 2.0  # h2, untouched
