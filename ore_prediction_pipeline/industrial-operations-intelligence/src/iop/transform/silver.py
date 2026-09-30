"""Stage: build_silver. Implemented in the Bronze/Silver build (spec 7.2)."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from iop.config import Config
from iop.models import StageResult


def run(spark: SparkSession, cfg: Config) -> StageResult:
    started = datetime.utcnow()
    return StageResult(
        stage="build_silver",
        rows_in=0,
        rows_out=0,
        status="stub",
        started_at=started,
        finished_at=datetime.utcnow(),
    )
