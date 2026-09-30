"""gold.fact_pipeline_runs (spec 12.1): one row per stage run, written by the
CLI after every stage so it works uniformly for stub and real stages alike."""

from __future__ import annotations

import uuid

from pyspark.sql import SparkSession

from iop.config import Config
from iop.delta_io import gold_location, merge_into
from iop.models import StageResult


def log_run(spark: SparkSession, cfg: Config, result: StageResult) -> None:
    run_id = result.run_id or str(uuid.uuid4())
    row = spark.createDataFrame(
        [
            (
                run_id,
                result.stage,
                result.started_at,
                result.finished_at,
                result.rows_in,
                result.rows_out,
                result.status,
            )
        ],
        ["run_id", "stage", "started_at", "finished_at", "rows_in", "rows_out", "status"],
    )
    merge_into(
        spark, cfg, row, gold_location(cfg, "fact_pipeline_runs"), ["run_id", "stage"]
    )
