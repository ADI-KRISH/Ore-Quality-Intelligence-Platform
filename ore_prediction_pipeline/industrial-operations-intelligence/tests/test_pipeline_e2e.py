"""Bronze -> Silver -> Gold on sample.csv, run twice: identical row counts and
no duplicate Gold keys (spec 7.3 "Done means" / DQ09)."""

from __future__ import annotations

from iop.ingest import bronze
from iop.quality import runner as dq_runner
from iop.transform import gold, silver


def _run_pipeline_once(spark, cfg):
    bronze.run(spark, cfg)
    silver_result = silver.run(spark, cfg)
    gold_result = gold.run(spark, cfg)
    return silver_result, gold_result


def test_pipeline_is_idempotent(spark, cfg):
    _, gold_1 = _run_pipeline_once(spark, cfg)
    _, gold_2 = _run_pipeline_once(spark, cfg)

    assert gold_1.status == "success"
    assert gold_2.status == "success"
    assert gold_1.rows_out == gold_2.rows_out

    fact_process_hourly = spark.read.format("delta").load(f"{cfg.paths.gold}/fact_process_hourly")
    assert fact_process_hourly.count() == fact_process_hourly.select("hour_ts", "plant_key").distinct().count()

    dim_time = spark.read.format("delta").load(f"{cfg.paths.gold}/dim_time")
    assert dim_time.count() == dim_time.select("hour_key").distinct().count()


def test_dq_checks_passes_after_a_clean_run(spark, cfg):
    _run_pipeline_once(spark, cfg)
    result = dq_runner.run(spark, cfg)
    assert result.status == "success", result.extra
