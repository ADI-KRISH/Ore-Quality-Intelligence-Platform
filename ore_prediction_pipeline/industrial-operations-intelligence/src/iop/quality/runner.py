"""Stage: dq_checks. DQ01-05/07/10 already run inline in build_silver, and
DQ06/08/09 in build_gold (each rule is quarantine-or-log, except DQ08/DQ09
which fail their own stage immediately). This stage is the final gate: after
a full pipeline run, refuse to call it done if gold.fact_dq_results recorded
any rule as failed."""

from __future__ import annotations

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from iop.config import Config
from iop.delta_io import gold_location
from iop.models import StageResult


def run(spark: SparkSession, cfg: Config) -> StageResult:
    started = datetime.utcnow()
    location = gold_location(cfg, "fact_dq_results")

    if cfg.is_local:
        exists = DeltaTable.isDeltaTable(spark, location)
        dq_results = spark.read.format("delta").load(location) if exists else None
    else:
        exists = spark.catalog.tableExists(location)
        dq_results = spark.table(location) if exists else None

    if dq_results is None:
        return StageResult(
            stage="dq_checks",
            rows_in=0,
            rows_out=0,
            status="failed",
            started_at=started,
            finished_at=datetime.utcnow(),
            extra={"reason": "fact_dq_results does not exist yet - run build_gold first"},
        )

    failed = dq_results.filter(~F.col("passed"))
    failed_count = failed.count()

    return StageResult(
        stage="dq_checks",
        rows_in=dq_results.count(),
        rows_out=dq_results.count() - failed_count,
        status="success" if failed_count == 0 else "failed",
        started_at=started,
        finished_at=datetime.utcnow(),
        extra={"failed_rules": [r.rule_id for r in failed.collect()]},
    )
