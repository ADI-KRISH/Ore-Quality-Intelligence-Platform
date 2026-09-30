"""Stage: train_or_load_model. Implemented in the ML build (spec 9)."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from iop.config import Config
from iop.models import StageResult


def run(spark: SparkSession, cfg: Config) -> StageResult:
    started = datetime.utcnow()
    return StageResult(
        stage="train_or_load_model",
        rows_in=0,
        rows_out=0,
        status="stub",
        started_at=started,
        finished_at=datetime.utcnow(),
    )
