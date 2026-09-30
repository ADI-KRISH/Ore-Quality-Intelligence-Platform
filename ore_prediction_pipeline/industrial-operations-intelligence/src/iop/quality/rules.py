"""Data-quality rules (spec 8). Thresholds come from config/dq.yaml, never
hardcoded (CLAUDE.md). Each function is pure: DataFrame in, DataFrame(s) out,
so every rule gets an isolated pass and fail unit test on sample.csv."""

from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from iop.transform.columns import CANONICAL_COLUMNS, NUMERIC_COLUMNS


def load_dq_config(path: str | Path = "config/dq.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def expand_ranges(ranges_cfg: dict) -> dict[str, tuple]:
    """dq.yaml uses one entry for all 7 air-flow / level columns; expand it."""
    expanded: dict[str, tuple] = {}
    for key, bounds in ranges_cfg.items():
        lo, hi = bounds
        if key == "column_air_flow":
            for n in range(1, 8):
                expanded[f"col{n:02d}_air_flow"] = (lo, hi)
        elif key == "column_level":
            for n in range(1, 8):
                expanded[f"col{n:02d}_level"] = (lo, hi)
        else:
            expanded[key] = (lo, hi)
    return expanded


def dq04_drop_exact_duplicates(bronze_df: DataFrame) -> tuple[DataFrame, dict]:
    """DQ04: exact duplicate rows removed; duplicate count logged (not quarantined)."""
    rows_checked = bronze_df.count()
    deduped = bronze_df.dropDuplicates(CANONICAL_COLUMNS)
    rows_after = deduped.count()
    stats = {
        "rule_id": "DQ04",
        "rows_checked": rows_checked,
        "rows_failed": rows_checked - rows_after,
        "passed": True,  # duplicates are logged, never fail the run
    }
    return deduped, stats


def _parse_numeric(col: Column) -> Column:
    """"55,2" -> 55.2. A comma is the only decimal separator in this dataset."""
    return F.regexp_replace(col, ",", ".").cast("double")


def apply_dq_rules(df: DataFrame, dq_cfg: dict) -> tuple[DataFrame, DataFrame, list[dict]]:
    """DQ02 (numeric parse), DQ03 (timestamp), DQ05 (physical ranges).

    Returns (typed_ok_df, quarantine_df, rule_stats). A row can fail more than
    one rule; it is quarantined once, under the first rule it fails, checked
    in the order DQ03 -> DQ02 -> DQ05.
    """
    ranges = expand_ranges(dq_cfg["ranges"])
    date_min, date_max = dq_cfg["date_range"]["min"], dq_cfg["date_range"]["max"]

    parsed = df.withColumn("ts_parsed", F.to_timestamp("ts", "yyyy-MM-dd HH:mm:ss"))
    for col in NUMERIC_COLUMNS:
        parsed = parsed.withColumn(f"{col}__parsed", _parse_numeric(F.col(col)))

    ts_ok = F.col("ts_parsed").isNotNull() & F.col("ts_parsed").between(date_min, date_max)
    numeric_ok = F.lit(True)
    for col in NUMERIC_COLUMNS:
        numeric_ok = numeric_ok & F.col(f"{col}__parsed").isNotNull()

    range_ok = F.lit(True)
    for col, (lo, hi) in ranges.items():
        value = F.col(f"{col}__parsed")
        bounds = F.lit(True)
        if lo is not None:
            bounds = bounds & (value >= lo)
        if hi is not None:
            bounds = bounds & (value <= hi)
        range_ok = range_ok & (value.isNull() | bounds)

    parsed = parsed.withColumn(
        "_dq_rule_id",
        F.when(~ts_ok, F.lit("DQ03"))
        .when(~numeric_ok, F.lit("DQ02"))
        .when(~range_ok, F.lit("DQ05"))
        .otherwise(F.lit(None)),
    )

    rows_checked = parsed.count()
    quarantine_df = (
        parsed.filter(F.col("_dq_rule_id").isNotNull())
        .select(*CANONICAL_COLUMNS, "_source_file", "_row_number", "_batch_id", "_dq_rule_id")
        .withColumnRenamed("_dq_rule_id", "rule_id")
    )

    ok_select = [F.col("ts_parsed").alias("ts")] + [
        F.col(f"{col}__parsed").alias(col) for col in NUMERIC_COLUMNS
    ]
    typed_ok_df = (
        parsed.filter(F.col("_dq_rule_id").isNull())
        .select(*ok_select, "_source_file", "_row_number", "_batch_id")
    )

    stats = [
        {
            "rule_id": rule_id,
            "rows_checked": rows_checked,
            "rows_failed": quarantine_df.filter(F.col("rule_id") == rule_id).count(),
            "passed": True,  # quarantine on failure, never fail the run (spec 8)
        }
        for rule_id in ("DQ03", "DQ02", "DQ05")
    ]
    return typed_ok_df, quarantine_df, stats
