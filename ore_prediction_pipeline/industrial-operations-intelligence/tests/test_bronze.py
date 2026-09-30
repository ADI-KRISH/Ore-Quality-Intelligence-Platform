from __future__ import annotations


def test_bronze_has_metadata_columns(bronze_df):
    cols = set(bronze_df.columns)
    assert {"_source_file", "_ingested_at", "_row_number", "_batch_id"} <= cols


def test_bronze_row_count_matches_file(bronze_df):
    # 714 real rows + 6 deliberately broken rows (tests/data/sample.csv)
    assert bronze_df.count() == 720


def test_bronze_values_are_untouched_strings(bronze_df):
    # Bronze corrects nothing: the decimal comma is still there.
    row = bronze_df.filter(bronze_df.starch_flow == "3019,53").first()
    assert row is not None
