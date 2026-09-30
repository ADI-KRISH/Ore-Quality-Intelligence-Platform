"""One passing and one failing case per DQ rule (CLAUDE.md), using the
deliberately broken rows in tests/data/sample.csv (see docs/PLAYBOOK.md Days 4-5)."""

from __future__ import annotations

from iop.quality.rules import apply_dq_rules, dq04_drop_exact_duplicates


def test_dq04_removes_the_one_injected_duplicate(bronze_df):
    deduped, stats = dq04_drop_exact_duplicates(bronze_df)
    assert stats["rule_id"] == "DQ04"
    assert stats["rows_failed"] == 1
    assert stats["passed"] is True  # duplicates are logged, never fail the run
    assert deduped.count() == bronze_df.count() - 1


def test_dq04_passes_distinct_rows(bronze_df):
    distinct_only = bronze_df.dropDuplicates(
        [c for c in bronze_df.columns if not c.startswith("_")]
    )
    deduped, stats = dq04_drop_exact_duplicates(distinct_only)
    assert stats["rows_failed"] == 0
    assert deduped.count() == distinct_only.count()


def test_dq02_quarantines_the_unparseable_numeric_row(bronze_df, dq_cfg):
    deduped, _ = dq04_drop_exact_duplicates(bronze_df)
    _, quarantine_df, _ = apply_dq_rules(deduped, dq_cfg)
    assert quarantine_df.filter(quarantine_df.rule_id == "DQ02").count() == 1


def test_dq02_passes_normal_rows(bronze_df, dq_cfg):
    import math

    deduped, _ = dq04_drop_exact_duplicates(bronze_df)
    typed_ok_df, _, _ = apply_dq_rules(deduped, dq_cfg)
    starch_values = [r.starch_flow for r in typed_ok_df.select("starch_flow").collect()]
    assert any(math.isclose(v, 3019.53, abs_tol=1e-6) for v in starch_values)


def test_dq03_quarantines_the_bad_timestamp_row(bronze_df, dq_cfg):
    deduped, _ = dq04_drop_exact_duplicates(bronze_df)
    _, quarantine_df, _ = apply_dq_rules(deduped, dq_cfg)
    assert quarantine_df.filter(quarantine_df.rule_id == "DQ03").count() == 1


def test_dq03_passes_valid_timestamps(bronze_df, dq_cfg):
    deduped, _ = dq04_drop_exact_duplicates(bronze_df)
    typed_ok_df, _, _ = apply_dq_rules(deduped, dq_cfg)
    assert typed_ok_df.count() > 700  # the vast majority parse fine


def test_dq05_quarantines_the_three_out_of_range_rows(bronze_df, dq_cfg):
    # bad pH (37,5), negative starch flow (-120,5), % Iron Feed > 100 (150,0)
    deduped, _ = dq04_drop_exact_duplicates(bronze_df)
    _, quarantine_df, _ = apply_dq_rules(deduped, dq_cfg)
    assert quarantine_df.filter(quarantine_df.rule_id == "DQ05").count() == 3


def test_dq05_passes_rows_within_range(bronze_df, dq_cfg):
    deduped, _ = dq04_drop_exact_duplicates(bronze_df)
    typed_ok_df, _, _ = apply_dq_rules(deduped, dq_cfg)
    assert typed_ok_df.filter((typed_ok_df.ore_pulp_ph >= 0) & (typed_ok_df.ore_pulp_ph <= 14)).count() == (
        typed_ok_df.count()
    )


def test_silver_pipeline_reconciles_to_original_row_count(bronze_df, dq_cfg):
    """714 real rows + 6 broken rows in; 1 duplicate (DQ04) + 5 rule failures
    (DQ02/DQ03/DQ05) out => exactly the 714 real rows should remain."""
    deduped, dq04_stats = dq04_drop_exact_duplicates(bronze_df)
    typed_ok_df, quarantine_df, _ = apply_dq_rules(deduped, dq_cfg)
    assert bronze_df.count() == dq04_stats["rows_failed"] + quarantine_df.count() + typed_ok_df.count()
    assert typed_ok_df.count() == 714
