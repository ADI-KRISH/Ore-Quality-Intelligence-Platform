from __future__ import annotations

import pytest

from iop.config import load_config
from iop.ingest.bronze import compute_checksum, read_raw_csv, to_bronze
from iop.quality.rules import apply_dq_rules, dq04_drop_exact_duplicates, load_dq_config
from iop.spark import get_spark


@pytest.fixture(scope="session")
def cfg():
    c = load_config("config/local.yaml")
    c.paths.raw_csv = "tests/data/sample.csv"
    return c


@pytest.fixture(scope="session")
def dq_cfg():
    return load_dq_config("config/dq.yaml")


@pytest.fixture(scope="module")
def spark(cfg, tmp_path_factory):
    """Module-scoped, not session-scoped: one fresh JVM/SparkSession per test
    FILE. A single long-lived session across the whole suite let query-plan
    and Delta-log complexity accumulate from earlier, heavier test files
    (test_pipeline_e2e's two full pipeline runs in particular) and poison
    later ones (test_sensor_health) with OOMs that had nothing to do with
    sensor_health's own logic. Restarting the JVM per file resets that."""
    warehouse = str(tmp_path_factory.mktemp("warehouse"))
    cfg.paths.warehouse_root = warehouse
    cfg.paths.bronze = f"{warehouse}/bronze"
    cfg.paths.silver = f"{warehouse}/silver"
    cfg.paths.gold = f"{warehouse}/gold"
    session = get_spark(cfg)
    yield session
    session.stop()


@pytest.fixture(scope="module")
def bronze_df(spark, cfg):
    raw = read_raw_csv(spark, cfg.paths.raw_csv)
    checksum = compute_checksum(cfg.paths.raw_csv)
    df = to_bronze(raw, source_file=cfg.paths.raw_csv, checksum=checksum, batch_id="test-batch").cache()
    df.count()  # materialize once; every test would otherwise recompute the read
    return df


@pytest.fixture(scope="module")
def dq_result(bronze_df, dq_cfg):
    """dq04 + apply_dq_rules, computed once and shared. Many tests only assert
    on a slice of this same result - recomputing it per-test was the actual
    driver of the OOMs seen in this suite (10+ redundant Spark jobs on an
    identical input within one long-lived JVM session), not a logic bug."""
    deduped, dq04_stats = dq04_drop_exact_duplicates(bronze_df)
    typed_ok_df, quarantine_df, dq_stats = apply_dq_rules(deduped, dq_cfg)
    typed_ok_df = typed_ok_df.cache()
    quarantine_df = quarantine_df.cache()
    typed_ok_df.count()
    quarantine_df.count()
    return {
        "deduped": deduped,
        "dq04_stats": dq04_stats,
        "typed_ok_df": typed_ok_df,
        "quarantine_df": quarantine_df,
        "dq_stats": dq_stats,
    }
