from __future__ import annotations

import pytest

from iop.config import load_config
from iop.ingest.bronze import read_raw_csv, to_bronze
from iop.quality.rules import load_dq_config
from iop.spark import get_spark


@pytest.fixture(scope="session")
def cfg():
    c = load_config("config/local.yaml")
    c.paths.raw_csv = "tests/data/sample.csv"
    return c


@pytest.fixture(scope="session")
def dq_cfg():
    return load_dq_config("config/dq.yaml")


@pytest.fixture(scope="session")
def spark(cfg, tmp_path_factory):
    warehouse = str(tmp_path_factory.mktemp("warehouse"))
    cfg.paths.warehouse_root = warehouse
    cfg.paths.bronze = f"{warehouse}/bronze"
    cfg.paths.silver = f"{warehouse}/silver"
    cfg.paths.gold = f"{warehouse}/gold"
    session = get_spark(cfg)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def bronze_df(spark, cfg):
    raw = read_raw_csv(spark, cfg.paths.raw_csv)
    return to_bronze(raw, source_file=cfg.paths.raw_csv, batch_id="test-batch").cache()
