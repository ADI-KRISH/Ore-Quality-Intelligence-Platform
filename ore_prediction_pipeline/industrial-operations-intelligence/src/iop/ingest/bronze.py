"""Stage: ingest_bronze (spec 7.1). Every column kept as a string; nothing is
dropped or corrected. Bronze is append-only - it is the audit trail - but
re-ingesting the exact same file (by content, not just name) is a no-op:
skipped and logged, not appended again. DQ01 (schema present) fails the run
before anything is written."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from iop.config import Config
from iop.delta_io import bronze_location, read_table, table_exists
from iop.models import StageResult
from iop.quality.rules import check_dq01_schema
from iop.transform.columns import RAW_TO_CANONICAL


def read_raw_csv(spark: SparkSession, csv_path: str):
    # No explicit schema: every column comes in as a string, matched by the
    # file's own header names (inferSchema defaults to false). to_bronze then
    # renames by name, so the raw file's column ORDER never matters here -
    # only RAW_TO_CANONICAL's keys need to match the header text.
    return spark.read.csv(csv_path, header=True, sep=",", quote='"')


def compute_checksum(path: str) -> str:
    """sha256 of the raw file's bytes - the "has this exact file already been
    loaded" question needs content, not just the path (which never changes
    here) or mtime (unreliable across filesystems/mounts)."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _already_loaded(spark: SparkSession, cfg: Config, source_file: str, checksum: str) -> bool:
    location = bronze_location(cfg)
    if not table_exists(spark, cfg, location):
        return False
    existing = read_table(spark, cfg, location).filter(
        (F.col("_source_file") == source_file) & (F.col("_source_checksum") == checksum)
    )
    return existing.limit(1).count() > 0


def to_bronze(raw_df, source_file: str, checksum: str, batch_id: str):
    df = raw_df
    for raw_name, canonical in RAW_TO_CANONICAL.items():
        df = df.withColumnRenamed(raw_name, canonical)
    return (
        df.withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_checksum", F.lit(checksum))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_row_number", F.monotonically_increasing_id())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def run(spark: SparkSession, cfg: Config) -> StageResult:
    started = datetime.utcnow()

    raw_df = read_raw_csv(spark, cfg.paths.raw_csv)
    dq01_passed, missing_columns = check_dq01_schema(raw_df.columns)
    if not dq01_passed:
        return StageResult(
            stage="ingest_bronze",
            rows_in=0,
            rows_out=0,
            status="failed",
            started_at=started,
            finished_at=datetime.utcnow(),
            extra={"dq01_missing_columns": missing_columns},
        )

    checksum = compute_checksum(cfg.paths.raw_csv)
    if _already_loaded(spark, cfg, cfg.paths.raw_csv, checksum):
        existing_rows = (
            read_table(spark, cfg, bronze_location(cfg))
            .filter(
                (F.col("_source_file") == cfg.paths.raw_csv)
                & (F.col("_source_checksum") == checksum)
            )
            .count()
        )
        return StageResult(
            stage="ingest_bronze",
            rows_in=existing_rows,
            rows_out=0,
            status="skipped",
            started_at=started,
            finished_at=datetime.utcnow(),
            extra={"reason": "source file already loaded (same path + checksum)"},
        )

    batch_id = str(uuid.uuid4())
    bronze_df = to_bronze(
        raw_df, source_file=cfg.paths.raw_csv, checksum=checksum, batch_id=batch_id
    )
    rows_in = bronze_df.count()

    if cfg.is_local:
        bronze_df.write.format("delta").mode("append").save(cfg.paths.bronze)
    else:
        table = bronze_location(cfg)
        bronze_df.write.format("delta").mode("append").saveAsTable(table)

    return StageResult(
        stage="ingest_bronze",
        rows_in=rows_in,
        rows_out=rows_in,
        status="success",
        started_at=started,
        finished_at=datetime.utcnow(),
        run_id=batch_id,
    )
