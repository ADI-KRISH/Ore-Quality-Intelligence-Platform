# Industrial Operations Intelligence Platform (IOP)

Flotation-plant % silica soft-sensor on a Delta Lake medallion pipeline.
Full spec: `docs/spec.md` (not auto-loaded; read the relevant section before planning any stage).
Owner decisions: @docs/decisions.md · Profiling findings: @docs/findings.md

## What we're building (one paragraph)
Hourly nowcast of % Silica Concentrate from 20-second process data, before the
hourly lab assay arrives. Raw CSV → Bronze → Silver (DQ + quarantine + sensor
health) → Gold (star schema, hourly features) → LightGBM (quantile p10/p50/p90,
SHAP) → PostgreSQL serving schema → Streamlit dashboard (+ Power BI report).
Runs locally (Docker, PySpark + delta-spark) and on Databricks Free Edition
(serverless) from ONE codebase; only config differs.

## Non-negotiable rules
- Stage code lives in `src/iop/`, every stage is a function `run(spark, cfg) -> StageResult`
  and must run unchanged locally and on Databricks. No `dbutils`, no hardcoded paths.
- Paths, catalog names, thresholds, dates, lab delay L → `config/*.yaml`. Never in code.
- Time-based splits ONLY. Never `train_test_split(shuffle=True)` or any random split
  (the only exception is the deliberate comparison inside `src/iop/ml/ablation.py`).
- `pct_iron_concentrate` is NEVER a model feature, except inside `src/iop/ml/ablation.py`.
- No feature may use data after the prediction cut-off. `tests/test_leakage.py` enforces it.
  If a metric looks too good, assume leakage and stop to investigate before continuing.
- The test period (Aug–Sep by default) is evaluated exactly once, by `iop evaluate --final`.
  Do not run it during development. Do not "peek".
- Never type metric values into README/docs. Generate them from `reports/metrics.json`.
- Every transform and DQ rule gets a unit test on `tests/data/sample.csv`
  (DQ rules: one passing and one failing test each). For DQ rules and the leakage
  check, write the tests FIRST.
- Bronze keeps every column as string + `_source_file, _ingested_at, _row_number, _batch_id`.
  Nothing is corrected in Bronze.
- Loads into Silver/Gold use Delta MERGE and must be idempotent (re-run = same row counts).
- Rejected rows go to `silver.quarantine` with rule id + batch id. Never silently drop rows.
- Never interpolate across a long gap. Never invent sub-hour timestamps.
- Synthetic scale-test plants (`dim_plant.is_synthetic = true`) never enter model training or evaluation.

## Working agreement with the owner
- ASK before adding any dependency not already in `pyproject.toml`.
- For any stage: plan first, wait for approval, then code. Don't write code in a planning turn.
- One stage per task. Small commits with clear messages. Run `make test` and `make lint`
  before saying a task is done, and show the result.
- When a real design choice comes up (lab delay, off-spec threshold, partial-hour
  handling, split boundaries, shift definition), STOP and ask the owner. Do not decide it.
  Once decided, the owner records it in `docs/decisions.md`.
- Explain non-obvious code in plain comments. Prefer readable over clever.
- Don't claim anything works that you didn't run.

## Commands
- `make setup`      create venv / install (uv or pip, see Makefile)
- `make test`       pytest (unit + small end-to-end on sample.csv)
- `make lint`       ruff check + ruff format --check
- `make pipeline`   `iop run --stage all --config config/local.yaml`
- `make dashboard`  streamlit run dashboard/app.py
- `iop run --stage <ingest_bronze|build_silver|dq_checks|build_gold|train_or_load_model|score_predictions|export_serving|refresh_reports|all>`

## Stack (fixed; don't swap without asking)
Python 3.11, PySpark + delta-spark (versions pinned together), LightGBM, XGBoost
(comparison), scikit-learn (Ridge, metrics), SHAP, MLflow, PostgreSQL 16,
SQLAlchemy/psycopg, Streamlit, pytest, ruff, Docker Compose, GitHub Actions.

## Canonical column names (Silver onward)
date→`ts`, % Iron Feed→`pct_iron_feed`, % Silica Feed→`pct_silica_feed`,
Starch Flow→`starch_flow`, Amina Flow→`amina_flow`, Ore Pulp Flow→`ore_pulp_flow`,
Ore Pulp pH→`ore_pulp_ph`, Ore Pulp Density→`ore_pulp_density`,
Flotation Column 0N Air Flow→`col0N_air_flow`, Flotation Column 0N Level→`col0N_level`,
% Iron Concentrate→`pct_iron_concentrate`, % Silica Concentrate→`pct_silica_concentrate` (TARGET).
Parse numbers with explicit decimal-comma handling. Never let the CSV reader infer types.

## Repo layout
See spec §13. Notebooks in `notebooks/` are exploration only; nothing imports from them.
