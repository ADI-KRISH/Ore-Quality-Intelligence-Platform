"""Stage: build_gold (spec 7.3). Star schema from Silver: dims + fact_process_hourly
(DQ06 completeness), fact_lab_quality (D04 off-spec), fact_dq_results (one row
per rule DQ01-DQ10 every run), with a DQ09 key-integrity check that fails the run."""

from __future__ import annotations

import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from iop.config import Config
from iop.delta_io import bronze_location, gold_location, merge_into, read_table, silver_location
from iop.models import StageResult
from iop.quality.rules import check_dq01_schema, load_dq_config
from iop.transform.dims import build_dim_plant, build_dim_sensor, build_dim_time
from iop.transform.facts import build_fact_lab_quality, build_fact_process_hourly, compute_off_spec_threshold


def _read_silver(spark: SparkSession, cfg: Config, table: str):
    return read_table(spark, cfg, silver_location(cfg, table))


def _latest_batch_id(bronze_df) -> str:
    row = bronze_df.select("_batch_id", "_ingested_at").orderBy(F.col("_ingested_at").desc()).first()
    return row["_batch_id"]


def run(spark: SparkSession, cfg: Config) -> StageResult:
    started = datetime.utcnow()
    run_id = str(uuid.uuid4())
    dq_cfg = load_dq_config()

    bronze_df = read_table(spark, cfg, bronze_location(cfg))
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

    # DQ08 (per batch, not cumulative): does THIS batch's bronze row count
    # reconcile with what it became in Silver (silver rows + quarantined +
    # duplicates removed)? Scoped to one batch because Silver reprocesses the
    # whole Bronze table every run - a cumulative check couldn't tell a stale
    # mismatch from a fresh one.
    latest_batch_id = _latest_batch_id(bronze_df)
    batch_bronze_count = bronze_df.filter(F.col("_batch_id") == latest_batch_id).count()
    batch_silver_count = process_readings.filter(F.col("_batch_id") == latest_batch_id).count()
    batch_quarantine_count = quarantine.filter(F.col("_batch_id") == latest_batch_id).count()
    batch_duplicates = batch_bronze_count - batch_silver_count - batch_quarantine_count
    dq08_passed = batch_duplicates >= 0

    # DQ01: re-check here (not just at ingest) so it gets a fact_dq_results
    # row every successful run, the same as every other rule.
    raw_columns = spark.read.csv(cfg.paths.raw_csv, header=True).columns
    dq01_passed, dq01_missing = check_dq01_schema(raw_columns)

    # DQ04: cumulative duplicate count across all of Bronze's history, logged
    # (never fails the run) - distinct from DQ08's per-batch reconciliation above.
    bronze_count = bronze_df.count()
    silver_count = process_readings.count()
    quarantine_count = quarantine.count()
    cumulative_duplicates = bronze_count - silver_count - quarantine_count

    quarantine_by_rule = {
        r.rule_id: r["count"] for r in quarantine.groupBy("rule_id").count().collect()
    }

    # DQ06: hours below the completeness threshold (D03) - logged, never fails
    # the run; ML excludes them from training instead.
    min_completeness = dq_cfg["hour_completeness"]["min_completeness_pct"]
    total_hours = fact_process_hourly.count()
    incomplete_hours = fact_process_hourly.filter(F.col("completeness_pct") < min_completeness).count()

    dq10_hours_flagged = lab_results.filter(F.col("lab_is_interpolated")).count()

    # Every row from this run shares the same checked_at, so dq_checks (and
    # anyone else) can select "the latest run's 10 rows" without a run_id -
    # fact_dq_results otherwise has no way to distinguish this run's rows
    # from every prior run's (the MERGE key is (rule_id, batch_id), so rows
    # only ever accumulate, never get superseded).
    checked_at = datetime.utcnow()
    dq_results_rows = [
        ("DQ01", run_id, checked_at, len(raw_columns), len(dq01_missing), dq01_passed),
        ("DQ02", run_id, checked_at, bronze_count, quarantine_by_rule.get("DQ02", 0), True),
        ("DQ03", run_id, checked_at, bronze_count, quarantine_by_rule.get("DQ03", 0), True),
        ("DQ04", run_id, checked_at, bronze_count, cumulative_duplicates, True),
        ("DQ05", run_id, checked_at, bronze_count, quarantine_by_rule.get("DQ05", 0), True),
        ("DQ06", run_id, checked_at, total_hours, incomplete_hours, True),
        # DQ07 ("exactly one lab value per hour") is superseded by D06: a
        # multi-value hour is no longer a failure, it's DQ10's interpolated
        # flag. Logged for traceability, always 0/passed.
        ("DQ07", run_id, checked_at, total_hours, 0, True),
        ("DQ08", run_id, checked_at, batch_bronze_count, batch_duplicates, dq08_passed),
        ("DQ09", run_id, checked_at, fact_process_hourly.count(), 0 if dq09_passed else 1, dq09_passed),
        ("DQ10", run_id, checked_at, lab_results.count(), dq10_hours_flagged, True),
    ]

    fact_dq_results = spark.createDataFrame(
        dq_results_rows,
        ["rule_id", "batch_id", "checked_at", "rows_checked", "rows_failed", "passed"],
    )
    merge_into(
        spark, cfg, fact_dq_results, gold_location(cfg, "fact_dq_results"),
        ["rule_id", "batch_id"],
    )

    status = "success" if (dq01_passed and dq08_passed and dq09_passed) else "failed"

    return StageResult(
        stage="build_gold",
        rows_in=silver_count,
        rows_out=fact_process_hourly.count(),
        status=status,
        started_at=started,
        finished_at=datetime.utcnow(),
        run_id=run_id,
        extra={
            "dq01_passed": dq01_passed,
            "dq08_passed": dq08_passed,
            "dq09_passed": dq09_passed,
            "dq10_hours_flagged": dq10_hours_flagged,
            "off_spec_threshold": off_spec_threshold,
        },
    )
