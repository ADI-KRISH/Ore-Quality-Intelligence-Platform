"""D06 / DQ10 (docs/decisions.md): an hour where the lab value varies almost
every row is flagged lab_is_interpolated, not treated as a normal hourly value."""

from __future__ import annotations

import pytest

from iop.transform.lab import build_lab_results


@pytest.fixture(scope="module")
def lab_results(dq_result, dq_cfg):
    return build_lab_results(dq_result["typed_ok_df"], dq_cfg).cache()


def test_varying_hour_is_flagged_interpolated(lab_results):
    row = lab_results.filter(lab_results.hour_ts == "2017-03-12 12:00:00").first()
    assert row is not None
    assert row.lab_is_interpolated is True


def test_normal_hour_is_not_flagged_interpolated(lab_results):
    row = lab_results.filter(lab_results.hour_ts == "2017-03-10 02:00:00").first()
    assert row is not None
    assert row.lab_is_interpolated is False
