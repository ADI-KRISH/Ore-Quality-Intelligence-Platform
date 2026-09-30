from __future__ import annotations

from iop.transform.columns import PROCESS_SENSOR_COLUMNS
from iop.transform.sensor_health import build_sensor_health


def test_sensor_health_covers_every_sensor_and_hour(dq_result, dq_cfg):
    typed_ok_df = dq_result["typed_ok_df"]
    health = build_sensor_health(typed_ok_df, dq_cfg)

    assert set(health.columns) == {"hour_ts", "sensor", "flatline", "spike"}
    n_hours = typed_ok_df.select("ts").distinct().count()
    # date_trunc("hour", ...) collapses to distinct hours, not distinct raw timestamps
    n_distinct_hours = health.select("hour_ts").distinct().count()
    assert health.count() == n_distinct_hours * len(PROCESS_SENSOR_COLUMNS)
    assert n_hours >= n_distinct_hours
    assert health.filter(health.flatline.isNull() | health.spike.isNull()).count() == 0
