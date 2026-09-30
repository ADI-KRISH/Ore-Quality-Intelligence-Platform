# Claude Code playbook — IOP build (21 days)

How to use: one phase = one or more Claude Code sessions. Paste the prompt, read the
plan, correct it, approve, let it build, then `/stage-done` and `/explain-back`.
Start a fresh session (`/clear`) between phases so old context doesn't leak in.
Use plan mode (Shift+Tab) for every "plan" prompt.

Loop for every stage:
```
/plan-stage <stage>      → you read + correct + answer the design questions
"Approved. Write the tests first, run them, show they fail. Then implement."
/stage-done <stage>      → you write docs/decisions.md entries yourself
/explain-back <stage>    → you answer the quiz without looking
git commit
```

---

## Day 1 — Scaffold, Docker, CI

```
Read CLAUDE.md and docs/spec.md sections 5, 6 and 13. Plan the repository scaffold. Don't write code yet.

I want:
- pyproject.toml (Python 3.11, package `iop` under src/, console script `iop` → src/iop/cli.py).
  Propose exact pinned versions for pyspark + delta-spark that are compatible with each other,
  and tell me how you verified compatibility.
- Makefile targets: setup, test, lint, pipeline, dashboard, up, down.
- docker-compose.yml: postgres:16 service + an app service with Java + PySpark + delta-spark.
  Streamlit can come later; leave a commented stub.
- src/iop/config.py: load a yaml into a typed dataclass/pydantic model; config/local.yaml and
  config/databricks.yaml with the same keys, different values.
- src/iop/spark.py: get_spark(cfg) that builds a local Delta-enabled session, or returns the
  active session on Databricks.
- src/iop/cli.py: `iop run --stage X --config path` with stage registry; stages are stubs for now.
- A StageResult dataclass (stage, rows_in, rows_out, status, started_at, finished_at).
- tests/: one smoke test that builds a Spark session and writes/reads a tiny Delta table.
- .github/workflows/ci.yml: ruff + pytest on push; needs Java for Spark.

List every dependency you intend to add and wait for my approval.
```

Also today (by hand, not Claude): create the Databricks Free Edition workspace. Then:
```
Give me a checklist to verify in my Databricks Free Edition workspace that I have: Jobs with
multiple tasks, MLflow tracking + model registry, Unity Catalog volumes for the raw CSV, and
the ability to pip-install our package (wheel) on serverless. For each, the exact UI steps
or a notebook cell to test it. Flag anything serverless compute is known to restrict
(e.g. Spark configs, caching) that could affect our design. Say clearly where you're unsure.
```

## Days 2–3 — Profiling (YOU drive, Claude assists)

Get the data first (Kaggle CLI or OpenML ID 43311) into `data/raw/`. Then:
```
Create notebooks/01_profiling.ipynb that tests each trap in docs/spec.md §4.1 and prints evidence.
Use pandas here (exploration only). Read the CSV with dtype=str first, then parse deliberately.
For each trap: one markdown cell stating the hypothesis, code, and a printed verdict with numbers.
Include: rows per distinct timestamp distribution, count of distinct values per hour for
lab/feed columns, missing hours/days list, correlation of pct_iron_concentrate with target,
MAE of the persistence forecast at lag 1 and 2 hours.
Don't write conclusions into docs/findings.md — I'll do that.
```
Then fill `docs/findings.md` yourself. Then pick D05 (split boundaries) based on the gaps you found.

## Days 4–5 — Bronze + Silver + DQ + sensor health

```
/plan-stage ingest_bronze and build_silver (spec §7.1, §7.2, §8 DQ01–DQ05 + DQ07, §10 flatline/out-of-range/spike)
```
After approval:
```
First create tests/data/sample.csv: ~3 days of real rows copied from the raw file, plus a handful of
deliberately broken rows (bad decimal, bad timestamp, pH out of range, negative flow, exact duplicate,
two different lab values in one hour). Keep it small. Show me the broken rows you added.
Then write the DQ tests (pass + fail per rule), run them, show they fail. Then implement.
DQ thresholds in config/dq.yaml. Rejected rows → silver.quarantine with rule_id and batch_id.
```

## Days 6–7 — Gold star schema

```
/plan-stage build_gold (spec §7.3, DQ06, DQ08, DQ09, fact_pipeline_runs)
```
Key prompts after approval:
```
Loads must be Delta MERGE on natural keys. Add an end-to-end test: run the full pipeline on
sample.csv twice and assert every Gold table has identical row counts and no duplicate keys.
```
```
Every stage run must append to gold.fact_pipeline_runs. Reconciliation (DQ08): bronze rows =
silver rows + quarantined rows + dropped exact duplicates. Fail the run if not.
```

## Day 8 — Same pipeline on Databricks

```
Plan how to run our package on Databricks Free Edition as a Job with one task per stage
(spec §12.1): build a wheel, upload to a UC volume, databricks/job.json task definitions,
config/databricks.yaml with catalog/schema names. No code changes to stage functions allowed —
if something only works locally, tell me what and why. Then write scripts/compare_counts.py
that prints local vs Databricks Gold row counts side by side.
```

## Days 9–11 — Features, baselines, LightGBM, MLflow

Decide D01 (lab delay L) and D03 before starting.
```
/leak-audit
```
(Should be mostly empty now — that's fine; it sets the bar.)
```
/plan-stage features + train (spec §9.1–9.4). Leakage test first.
```
After approval:
```
Write tests/test_leakage.py FIRST. It must build features for a known hour h and assert:
(a) no process feature uses rows after end of hour h, (b) no lab feature uses hours after h − L,
(c) pct_iron_concentrate is not in the feature list, (d) for horizon k, target is at h+k and
features still stop at h. Include a deliberately leaky feature function and assert the test catches it.
Run it, show it fails for the leaky version, then implement features.py.
```
```
Implement persistence, Ridge, LightGBM (point + quantile 0.1/0.9) and XGBoost with walk-forward
expanding-window CV on train+val only. Log everything to MLflow (params, features_hash, CV MAE,
skill vs persistence). Do NOT touch the test period. Print a validation comparison table.
```
Then: `/leak-audit` again. If validation skill looks huge, stop and dig.

## Day 12 — Final evaluation (ONCE)

```
Plan `iop evaluate --final` and src/iop/ml/ablation.py: test-set MAE/RMSE, skill vs persistence,
off-spec precision/recall (threshold from docs/decisions.md D04), p10–p90 coverage, horizon curve
k=0..4, SHAP global + per-hour top 3, and the 2×2 ablation table (with/without iron × time/random split).
Everything written to reports/metrics.json and reports/figures/. The final run must refuse to run
twice unless --force, and log a separate MLflow run tagged final=true.
```
Before running it: `/leak-audit`. Then run it yourself and read every number.

## Day 13 — PostgreSQL serving

```
/plan-stage export_serving (spec §11.1): database/schema.sql with PKs, FKs, indexes on hour_key;
roles.sql with app_rw and analyst_ro; upsert export from Gold. Include a test that connects as
analyst_ro and asserts INSERT/UPDATE/DELETE all fail.
```

## Days 14–16 — Streamlit dashboard

```
/plan-stage dashboard (spec §11.2): five pages + replay control. The dashboard reads ONLY from
Postgres via analyst_ro. Replay must only show data available at the replay hour (predictions
and lab values up to h − L). Show me a wireframe (text) for each page before coding.
```
Deploy: Streamlit Community Cloud + hosted Postgres (e.g. Supabase free tier). Secrets via
Streamlit secrets, never committed.

## Day 17 — Power BI (by hand)
Power BI Desktop on Windows against the serving tables. Claude can help with DAX:
```
Write DAX measures for: off-spec rate by shift, rolling 7-day MAE, prediction interval coverage.
Tables: serving.fact_predictions, serving.fact_lab_quality, serving.dim_time.
```

## Day 18 — Monitoring + scale test

```
/plan-stage monitoring (spec §12.2: rolling MAE, PSI on top-5 features, retraining trigger in config)
```
```
/plan-stage scale_test (spec §12.3): scripts/scale_test.py generating N synthetic plants
(time-shifted + noise, is_synthetic=true), runtime at 1×, 10×, 50× locally and on Databricks,
written to reports/scale.json. Assert synthetic plants are excluded from ML.
```

## Days 19–20 — README, diagram, demo

```
Generate README.md sections from files: metrics table from reports/metrics.json, scale table
from reports/scale.json, findings from docs/findings.md. Write scripts/build_readme.py so the
numbers can be regenerated; add a CI check that fails if README numbers drift from metrics.json.
Architecture diagram as Mermaid.
```
Write the resume bullets yourself using only numbers from `reports/metrics.json`.

## Day 21 — Rehearsal
```
Act as a skeptical Aditya Birla GDNA technical interviewer. Using this repo, ask me the spec §17.2
questions plus 5 you pick from the code, one at a time. Push back on vague answers.
```

---

## If things go sideways
- Claude added a dependency without asking → "Revert that. Ask first, per CLAUDE.md."
- Metric suspiciously good → `/leak-audit`, then "Explain exactly which rows each feature for hour X came from."
- Session got long and sloppy → `/compact` or `/clear`, re-state the current stage.
- You can't explain a module → don't move on. `/explain-back <module>` until you can.
