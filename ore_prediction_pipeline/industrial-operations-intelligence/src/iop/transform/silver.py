"""Stage: build_silver (spec 7.2). Bronze -> typed, DQ-checked, hourly Silver
tables. DQ01 (schema) is enforced by Bronze's fixed StructType at read time;
DQ04, DQ02/03/05 and DQ10 (D06) run here."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from iop.config import Config
from iop.delta_io import bronze_location, merge_into, read_table, silver_location
from iop.models import StageResult
from iop.quality.rules import apply_dq_rules, dq04_drop_exact_duplicates, load_dq_config
from iop.transform.lab import build_feed_quality, build_lab_results
from iop.transform.sensor_health import build_sensor_health


def run(spark: SparkSession, cfg: Config) -> StageResult:
    started = datetime.utcnow()
    dq_cfg = load_dq_config()

    bronze_df = read_table(spark, cfg, bronze_location(cfg))
    rows_in = bronze_df.count()

    deduped_df, dq04_stats = dq04_drop_exact_duplicates(bronze_df)
    typed_ok_df, quarantine_df, dq_stats = apply_dq_rules(deduped_df, dq_cfg)
    typed_ok_df = typed_ok_df.cache()

    process_readings = typed_ok_df.withColumn(
        "hour_ts", F.date_trunc("hour", F.col("ts"))
    ).withColumn("dq_status", F.lit("ok"))
    lab_results = build_lab_results(typed_ok_df, dq_cfg)
    feed_quality = build_feed_quality(typed_ok_df)
    sensor_health = build_sensor_health(typed_ok_df, dq_cfg)

    merge_into(
        spark,
        cfg,
        process_readings,
        silver_location(cfg, "process_readings"),
        ["_source_file", "_row_number"],
    )
    merge_into(
        spark,
        cfg,
        quarantine_df,
        silver_location(cfg, "quarantine"),
        ["_source_file", "_row_number"],
    )
    merge_into(spark, cfg, lab_results, silver_location(cfg, "lab_results"), ["hour_ts"])
    merge_into(spark, cfg, feed_quality, silver_location(cfg, "feed_quality"), ["hour_ts"])
    merge_into(
        spark,
        cfg,
        sensor_health,
        silver_location(cfg, "sensor_health"),
        ["hour_ts", "sensor"],
    )

    rows_out = typed_ok_df.count()
    dq10_hours_flagged = lab_results.filter(F.col("lab_is_interpolated")).count()
    typed_ok_df.unpersist()

    return StageResult(
        stage="build_silver",
        rows_in=rows_in,
        rows_out=rows_out,
        status="success",
        started_at=started,
        finished_at=datetime.utcnow(),
        extra={
            "dq04_duplicates_removed": dq04_stats["rows_failed"],
            "dq_rule_stats": dq_stats,
            "dq10_hours_flagged": dq10_hours_flagged,
        },
    )
