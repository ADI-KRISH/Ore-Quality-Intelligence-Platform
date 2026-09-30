"""Smoke test: local Spark session builds and Delta read/write round-trips."""

from __future__ import annotations

from iop.config import load_config
from iop.spark import get_spark


def test_spark_delta_roundtrip(tmp_path):
    cfg = load_config("config/local.yaml")
    cfg.paths.warehouse_root = str(tmp_path)
    spark = get_spark(cfg)

    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    table_path = str(tmp_path / "smoke_table")
    df.write.format("delta").mode("overwrite").save(table_path)

    result = spark.read.format("delta").load(table_path)
    assert result.count() == 2
    assert sorted(row["value"] for row in result.collect()) == ["a", "b"]

    spark.stop()
