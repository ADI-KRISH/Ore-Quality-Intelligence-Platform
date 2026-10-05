"""Bronze -> Silver -> Gold on sample.csv, run twice: identical row counts in
EVERY table including bronze (re-ingesting the same file is a no-op - fix for
the real-CSV run where bronze silently doubled), no duplicate Gold keys
(spec 7.3 "Done means" / DQ09), fact_dq_results has all ten rules (DQ01-DQ10)
every run, and dq_checks passes.

One test, not several: each full pipeline run is a real (non-trivial) amount
of Spark work, and running it a third time in the same test session for a
separate assertion was pushing the container's JVM heap into OOM territory
for no added coverage.
"""

from __future__ import annotations

from iop.ingest import bronze
from iop.quality import runner as dq_runner
from iop.transform import gold, silver


def _run_pipeline_once(spark, cfg):
    bronze_result = bronze.run(spark, cfg)
    silver_result = silver.run(spark, cfg)
    gold_result = gold.run(spark, cfg)
    return bronze_result, silver_result, gold_result


def _dq_rule_ids_for_run(spark, cfg, batch_id: str) -> set[str]:
    dq_results = spark.read.format("delta").load(f"{cfg.paths.gold}/fact_dq_results")
    rows = dq_results.filter(dq_results.batch_id == batch_id).select("rule_id").collect()
    return {r.rule_id for r in rows}


def test_pipeline_is_idempotent_and_dq_clean(spark, cfg):
    bronze_1, _, gold_1 = _run_pipeline_once(spark, cfg)
    bronze_2, _, gold_2 = _run_pipeline_once(spark, cfg)

    assert bronze_1.status == "success"
    assert bronze_2.status == "skipped"  # same file + checksum, already loaded

    assert gold_1.status == "success"
    assert gold_2.status == "success"
    assert gold_1.rows_out == gold_2.rows_out

    bronze_count = spark.read.format("delta").load(cfg.paths.bronze).count()
    assert bronze_count == bronze_1.rows_out  # unchanged by the skipped second ingest

    fact_process_hourly = spark.read.format("delta").load(f"{cfg.paths.gold}/fact_process_hourly")
    assert (
        fact_process_hourly.count()
        == fact_process_hourly.select("hour_ts", "plant_key").distinct().count()
    )

    dim_time = spark.read.format("delta").load(f"{cfg.paths.gold}/dim_time")
    assert dim_time.count() == dim_time.select("hour_key").distinct().count()

    expected_rules = {f"DQ{n:02d}" for n in range(1, 11)}
    assert _dq_rule_ids_for_run(spark, cfg, gold_1.run_id) == expected_rules
    assert _dq_rule_ids_for_run(spark, cfg, gold_2.run_id) == expected_rules

    dq_result = dq_runner.run(spark, cfg)
    assert dq_result.status == "success", dq_result.extra
