"""Smoke test: Delta read/write round-trips on the shared Spark session."""

from __future__ import annotations


def test_spark_delta_roundtrip(spark, tmp_path):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    table_path = str(tmp_path / "smoke_table")
    df.write.format("delta").mode("overwrite").save(table_path)

    result = spark.read.format("delta").load(table_path)
    assert result.count() == 2
    assert sorted(row["value"] for row in result.collect()) == ["a", "b"]
