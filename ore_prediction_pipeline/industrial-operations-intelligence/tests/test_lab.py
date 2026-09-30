"""D06 / DQ10 (docs/decisions.md): an hour where the lab value varies almost
every row is flagged lab_is_interpolated, not treated as a normal hourly value."""

from __future__ import annotations

from iop.quality.rules import apply_dq_rules, dq04_drop_exact_duplicates
from iop.transform.lab import build_lab_results


def test_varying_hour_is_flagged_interpolated(bronze_df, dq_cfg):
    deduped, _ = dq04_drop_exact_duplicates(bronze_df)
    typed_ok_df, _, _ = apply_dq_rules(deduped, dq_cfg)
    lab_results = build_lab_results(typed_ok_df, dq_cfg)

    row = lab_results.filter(lab_results.hour_ts == "2017-03-12 12:00:00").first()
    assert row is not None
    assert row.lab_is_interpolated is True


def test_normal_hour_is_not_flagged_interpolated(bronze_df, dq_cfg):
    deduped, _ = dq04_drop_exact_duplicates(bronze_df)
    typed_ok_df, _, _ = apply_dq_rules(deduped, dq_cfg)
    lab_results = build_lab_results(typed_ok_df, dq_cfg)

    row = lab_results.filter(lab_results.hour_ts == "2017-03-10 02:00:00").first()
    assert row is not None
    assert row.lab_is_interpolated is False
