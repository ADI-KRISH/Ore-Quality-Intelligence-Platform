"""Typed config loading. Stage code never reads yaml directly, never hardcodes
paths/thresholds/dates - everything comes through Config so local and
Databricks runs differ only by which yaml file is passed in."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel


class PathsConfig(BaseModel):
    raw_csv: str
    warehouse_root: str | None = None
    bronze: str | None = None
    silver: str | None = None
    gold: str | None = None


class CatalogConfig(BaseModel):
    name: str | None = None
    schema_bronze: str | None = None
    schema_silver: str | None = None
    schema_gold: str | None = None


class PostgresConfig(BaseModel):
    host: str | None = None
    host_env: str | None = None
    port: int
    database: str
    app_rw_user: str
    app_rw_password_env: str
    analyst_ro_user: str
    analyst_ro_password_env: str


class SplitConfig(BaseModel):
    train_start: str
    train_end: str
    val_start: str
    val_end: str
    test_start: str
    test_end: str


class MlConfig(BaseModel):
    lab_delay_hours: int
    off_spec_percentile: int
    shift_starts_hour: int
    min_hour_completeness_pct: int
    split: SplitConfig
    horizons: list[int]


class MlflowConfig(BaseModel):
    tracking_uri: str
    experiment_name: str


class Config(BaseModel):
    mode: str  # "local" | "databricks"
    paths: PathsConfig
    catalog: CatalogConfig
    postgres: PostgresConfig
    ml: MlConfig
    mlflow: MlflowConfig

    @property
    def is_local(self) -> bool:
        return self.mode == "local"


def load_config(path: str | Path) -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return Config.model_validate(raw)
