"""Idempotent Delta MERGE, shared by Silver and Gold. One location resolver +
one merge function so every stage writes the same way in both run modes."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from iop.config import Config


def bronze_location(cfg: Config) -> str:
    if cfg.is_local:
        return cfg.paths.bronze
    return f"{cfg.catalog.name}.{cfg.catalog.schema_bronze}.flotation_raw"


def silver_location(cfg: Config, table: str) -> str:
    if cfg.is_local:
        return f"{cfg.paths.silver}/{table}"
    return f"{cfg.catalog.name}.{cfg.catalog.schema_silver}.{table}"


def gold_location(cfg: Config, table: str) -> str:
    if cfg.is_local:
        return f"{cfg.paths.gold}/{table}"
    return f"{cfg.catalog.name}.{cfg.catalog.schema_gold}.{table}"


def table_exists(spark: SparkSession, cfg: Config, location: str) -> bool:
    if cfg.is_local:
        return DeltaTable.isDeltaTable(spark, location)
    return spark.catalog.tableExists(location)


def read_table(spark: SparkSession, cfg: Config, location: str) -> DataFrame:
    if cfg.is_local:
        return spark.read.format("delta").load(location)
    return spark.table(location)


def merge_into(
    spark: SparkSession, cfg: Config, df: DataFrame, location: str, keys: list[str]
) -> None:
    """Insert new keys, update matching ones. Re-running on the same input
    data is a no-op (same row counts), which is what "idempotent" means here.

    localCheckpoint materializes df and truncates its lineage first. Without
    it, a source built from apply_dq_rules' many chained column expressions,
    fed into repeated MERGEs across a full pipeline run (worse, twice, for an
    idempotency test), grows a query plan deep enough for Catalyst's tree
    traversal itself to exhaust driver heap - a real crash seen in this
    project's test suite, not a hypothetical."""
    df = df.localCheckpoint(eager=True)
    if not table_exists(spark, cfg, location):
        writer = df.write.format("delta")
        if cfg.is_local:
            writer.save(location)
        else:
            writer.saveAsTable(location)
        return

    target = (
        DeltaTable.forPath(spark, location) if cfg.is_local else DeltaTable.forName(spark, location)
    )
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        target.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
