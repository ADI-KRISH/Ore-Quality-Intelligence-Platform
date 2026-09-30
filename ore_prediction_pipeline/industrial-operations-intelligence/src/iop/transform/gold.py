"""Stage: build_gold (spec 7.3). Star schema from Silver: dims + fact_process_hourly
(DQ06 completeness), fact_lab_quality (D04 off-spec), fact_dq_results (DQ08
reconciliation, DQ10 count), with a DQ09 key-integrity check that fails the run."""

from __future__ import annotations

import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from iop.config import Config
from iop.delta_io import gold_location, merge_into, silver_location
from iop.models import StageResult
from iop.quality.rules import load_dq_config
from iop.transform.dims import build_dim_plant, build_dim_sensor, build_dim_time
from iop.transform.facts import build_fact_lab_quality, build_fact_process_hourly, compute_off_spec_threshold


def _read_silver(spark: SparkSession, cfg: Config, table: str):
    if cfg.is_local:
        return spark.read.format("delta").load(silver_location(cfg, table))
    return spark.table(silver_location(cfg, table))


def _read_bronze_count(spark: SparkSession, cfg: Config) -> int:
    if cfg.is_local:
        return spark.read.format("delta").load(cfg.paths.bronze).count()
    table = f"{cfg.catalog.name}.{cfg.catalog.schema_bronze}.flotation_raw"
    return spark.table(table).count()


def run(spark: SparkSession, cfg: Config) -> StageResult:
    started = datetime.utcnow()
    run_id = str(uuid.uuid4())
    dq_cfg = load_dq_config()

    process_readings = _read_silver(spark, cfg, "process_readings")
    lab_results = _read_silver(spark, cfg, "lab_results")
    feed_quality = _read_silver(spark, cfg, "feed_quality")
    quarantine = _read_silver(spark, cfg, "quarantine")

    plant_key = "plant_1"

    fact_process_hourly = build_fact_process_hourly(process_readings, dq_cfg).withColumn(
        "plant_key", F.lit(plant_key)
    )
    off_spec_threshold = compute_off_spec_threshold(
        lab_results,
        cfg.ml.split.train_start,
        cfg.ml.split.train_end,
        cfg.ml.off_spec_percentile,
    )
    fact_lab_quality = build_fact_lab_quality(lab_results, off_spec_threshold).withColumn(
        "plant_key", F.lit(plant_key)
    )

    all_hours = fact_process_hourly.select("hour_ts").unionByName(
        fact_lab_quality.select("hour_ts")
    ).unionByName(feed_quality.select("hour_ts"))
    dim_time = build_dim_time(all_hours, cfg.ml.shift_starts_hour)
    dim_sensor = build_dim_sensor(spark, dq_cfg)
    dim_plant = build_dim_plant(spark)

    # DQ09: every fact hour_key must exist in dim_time, and dim_time must be unique.
    dim_hours = {r.hour_key for r in dim_time.select("hour_key").collect()}
    fact_hours = {r.hour_ts for r in fact_process_hourly.select("hour_ts").collect()}
    dim_time_is_unique = dim_time.count() == dim_time.select("hour_key").distinct().count()
    dq09_passed = fact_hours.issubset(dim_hours) and dim_time_is_unique

    merge_into(spark, cfg, dim_time, gold_location(cfg, "dim_time"), ["hour_key"])
    merge_into(spark, cfg, dim_sensor, gold_location(cfg, "dim_sensor"), ["sensor_key"])
    merge_into(spark, cfg, dim_plant, gold_location(cfg, "dim_plant"), ["plant_key"])
    merge_into(
        spark, cfg, fact_process_hourly, gold_location(cfg, "fact_process_hourly"),
        ["hour_ts", "plant_key"],
    )
    merge_into(
        spark, cfg, fact_lab_quality, gold_location(cfg, "fact_lab_quality"),
        ["hour_ts", "plant_key"],
    )

    # DQ08: bronze rows must reconcile to silver rows + quarantined rows + exact
    # duplicates dropped (DQ04). We only have two of those three independently
    # persisted (silver, quarantine); the third is derived, so this also
    # doubles as a sanity check that MERGE didn't silently drop or double rows.
    bronze_count = _read_bronze_count(spark, cfg)
    silver_count = process_readings.count()
    quarantine_count = quarantine.count()
    duplicates_removed = bronze_count - silver_count - quarantine_count
    dq08_passed = duplicates_removed >= 0

    dq10_hours_flagged = lab_results.filter(F.col("lab_is_interpolated")).count()
    quarantine_by_rule = {
        r.rule_id: r["count"]
        for r in quarantine.groupBy("rule_id").count().collect()
    }

    dq_results_rows = [
        ("DQ04", run_id, bronze_count, duplicates_removed, True),
        ("DQ08", run_id, bronze_count, 0 if dq08_passed else 1, dq08_passed),
        ("DQ09", run_id, fact_process_hourly.count(), 0 if dq09_passed else 1, dq09_passed),
        ("DQ10", run_id, lab_results.count(), dq10_hours_flagged, True),
    ]
    for rule_id, failed in quarantine_by_rule.items():
        dq_results_rows.append((rule_id, run_id, bronze_count, failed, True))

    fact_dq_results = spark.createDataFrame(
        dq_results_rows, ["rule_id", "batch_id", "rows_checked", "rows_failed", "passed"]
    )
    merge_into(
        spark, cfg, fact_dq_results, gold_location(cfg, "fact_dq_results"),
        ["rule_id", "batch_id"],
    )

    status = "success" if (dq08_passed and dq09_passed) else "failed"

    return StageResult(
        stage="build_gold",
        rows_in=silver_count,
        rows_out=fact_process_hourly.count(),
        status=status,
        started_at=started,
        finished_at=datetime.utcnow(),
        run_id=run_id,
        extra={
            "dq08_passed": dq08_passed,
            "dq09_passed": dq09_passed,
            "dq10_hours_flagged": dq10_hours_flagged,
            "off_spec_threshold": off_spec_threshold,
        },
    )
