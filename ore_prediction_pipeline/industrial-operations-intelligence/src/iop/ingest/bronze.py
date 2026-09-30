"""Stage: ingest_bronze (spec 7.1). Every column kept as a string; nothing is
dropped or corrected. Bronze is append-only - it is the audit trail, so unlike
Silver/Gold it does not need idempotent MERGE (re-ingesting appends another
batch, which is the point: you can always see what was ingested when)."""

from __future__ import annotations

import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from iop.config import Config
from iop.models import StageResult
from iop.transform.columns import RAW_TO_CANONICAL


def read_raw_csv(spark: SparkSession, csv_path: str):
    # No explicit schema: every column comes in as a string, matched by the
    # file's own header names (inferSchema defaults to false). to_bronze then
    # renames by name, so the raw file's column ORDER never matters here -
    # only RAW_TO_CANONICAL's keys need to match the header text.
    return spark.read.csv(csv_path, header=True, sep=",", quote='"')


def to_bronze(raw_df, source_file: str, batch_id: str):
    df = raw_df
    for raw_name, canonical in RAW_TO_CANONICAL.items():
        df = df.withColumnRenamed(raw_name, canonical)
    return (
        df.withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_row_number", F.monotonically_increasing_id())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def run(spark: SparkSession, cfg: Config) -> StageResult:
    started = datetime.utcnow()
    batch_id = str(uuid.uuid4())

    raw_df = read_raw_csv(spark, cfg.paths.raw_csv)
    bronze_df = to_bronze(raw_df, source_file=cfg.paths.raw_csv, batch_id=batch_id)
    rows_in = bronze_df.count()

    if cfg.is_local:
        bronze_df.write.format("delta").mode("append").save(cfg.paths.bronze)
    else:
        table = f"{cfg.catalog.name}.{cfg.catalog.schema_bronze}.flotation_raw"
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
