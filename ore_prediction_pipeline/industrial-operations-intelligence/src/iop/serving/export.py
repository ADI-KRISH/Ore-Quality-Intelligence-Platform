"""Stage: export_serving. Implemented in the PostgreSQL serving build (spec 11.1)."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from iop.config import Config
from iop.models import StageResult


def run(spark: SparkSession, cfg: Config) -> StageResult:
    started = datetime.utcnow()
    return StageResult(
        stage="export_serving",
        rows_in=0,
        rows_out=0,
        status="stub",
        started_at=started,
        finished_at=datetime.utcnow(),
    )
