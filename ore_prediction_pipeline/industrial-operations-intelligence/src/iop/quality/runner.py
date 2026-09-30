"""Stage: dq_checks. DQ01-05/07/10 already run inline in build_silver, and
DQ06/08/09 in build_gold (each rule is quarantine-or-log, except DQ01/DQ08/DQ09
which can fail their own stage immediately). This stage is the final gate:
after a full pipeline run, refuse to call it done if the LATEST run's rows in
gold.fact_dq_results recorded any rule as failed - not any run ever, since
fact_dq_results only ever accumulates rows (MERGE key is rule_id + batch_id),
so a stale failure from months ago must never fail today's run."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from iop.config import Config
from iop.delta_io import gold_location, read_table, table_exists
from iop.models import StageResult


def run(spark: SparkSession, cfg: Config) -> StageResult:
    started = datetime.utcnow()
    location = gold_location(cfg, "fact_dq_results")

    if not table_exists(spark, cfg, location):
        return StageResult(
            stage="dq_checks",
            rows_in=0,
            rows_out=0,
            status="failed",
            started_at=started,
            finished_at=datetime.utcnow(),
            extra={"reason": "fact_dq_results does not exist yet - run build_gold first"},
        )

    dq_results = read_table(spark, cfg, location)
    latest_checked_at = dq_results.agg(F.max("checked_at")).first()[0]
    latest = dq_results.filter(F.col("checked_at") == latest_checked_at)

    failed = latest.filter(~F.col("passed"))
    failed_count = failed.count()

    return StageResult(
        stage="dq_checks",
        rows_in=latest.count(),
        rows_out=latest.count() - failed_count,
        status="success" if failed_count == 0 else "failed",
        started_at=started,
        finished_at=datetime.utcnow(),
        extra={"failed_rules": [r.rule_id for r in failed.collect()]},
    )
