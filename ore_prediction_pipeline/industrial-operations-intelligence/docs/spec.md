**Industrial Operations Intelligence Platform**

Flotation-plant quality soft-sensor on a Databricks lakehouse

| **Item** | **Detail** |
| --- | --- |
| Version | v2 build specification (replaces v1 concept note) |
| Owner | GS Adithya Krishna |
| Target role | Aditya Birla Group, GDNA PRIME Technology Trainee (Data Engineering + Data Science) |
| Build tool | Claude Code, with every module reviewed and understood by the owner |
| Timeline | 3 weeks for the core system; stretch goals only after the core is done |
| One-line pitch | A pipeline and model that estimates impurity in iron-ore concentrate every hour, before the lab result arrives, so operators can correct the process earlier. |

| **The rule this document is built on** Depth beats breadth. v1 listed ten technologies and two ML problems with no dataset. v2 picks **one real problem, one real dataset, one cloud platform and one model**, and does the engineering around them properly. Every claim that ends up on the resume must be backed by something in the repository. |
| --- |

# 1. What changed from v1 and why

| **Area** | **v1** | **v2** | **Why** |
| --- | --- | --- | --- |
| Problem | "Industrial operations intelligence" (no concrete problem) | Nowcast % silica in flotation concentrate before the hourly lab assay | An interviewer's first question is "what problem, for whom, on what data?" v2 has an answer. |
| Data | Synthetic or unnamed schema | Real flotation-plant data (Kaggle / OpenML, CC0), Mar–Sep 2017 | Real plant data has real mess: mixed sampling rates, gaps, leaking columns. That mess is the data-engineering story. |
| Cloud | AWS S3, EC2, RDS | Databricks Free Edition (primary); Azure optional | The JD names Azure and Databricks, not AWS. Free Edition costs nothing. |
| Lake + warehouse | Separate lake, Spark, PostgreSQL warehouse | Delta Lake medallion on Databricks; PostgreSQL only as the serving layer | Databricks already gives lake, warehouse and scheduling. Stacking extra systems is complexity for show. |
| Orchestration | Airflow | Databricks Jobs in the cloud; a single CLI locally | Same DAG idea, one fewer system to run and defend. |
| Streaming | Kafka + Structured Streaming | Removed from core. Optional file-replay stretch goal | No real live source exists. Kafka on generated data is theatre and is not in the JD. |
| ML scope | Predictive maintenance + anomaly detection | One model (quality soft-sensor) + rule-based sensor-health checks | One model evaluated rigorously beats two evaluated loosely. |
| Evaluation | Generic metrics list | Time-based split, persistence baseline, leakage ablation, forecast-horizon study | This is where most public notebooks on this dataset go wrong, so it is where you stand out. |

# 2. Business problem

In reverse flotation of iron ore, silica is the impurity. Concentrate quality is judged mainly by **% silica**, and it is measured by lab assay roughly once an hour. Between assays, operators adjust reagent dosing (starch, amine), pulp flow and column air and level with no direct view of the quality they are producing. When a lab result comes back off-spec, an hour or more of production has already been made at that quality.

The platform turns the plant's 20-second process data into an **hourly quality estimate that arrives before the lab result**, with an uncertainty band and an explanation of which process variables are driving it. The same pipeline also tracks sensor health and pipeline health, because a model is only as trustworthy as the data feeding it.

| **Stakeholder** | **Question the platform answers** | **Where they see it** |
| --- | --- | --- |
| Shift operator | Is this hour's concentrate likely off-spec, and what is pushing it there? | Dashboard: Quality Now, Drivers |
| Process engineer | Which variables drive silica over weeks? How far ahead can we predict? | Dashboard: Trends, Drivers; horizon study report |
| Data team (you) | Is the data fresh and valid? Is the model degrading? | Dashboard: Pipeline & Model Health |

| **Why this lands with Aditya Birla Group** GDNA serves plant-heavy businesses in metals, cement, chemicals and textiles. A quality soft-sensor for a mineral processing plant is exactly the kind of use case those businesses run. In the interview you can say: "this is what I would build for one of the Group's plants, and here is how it would change with your data." |
| --- |

# 3. Success criteria

The project is done when every row below is true. These are the acceptance tests for the whole build.

| **Area** | **Done means** |
| --- | --- |
| Pipeline | One command rebuilds Bronze → Silver → Gold from the raw file, locally and on Databricks, with identical row counts. Re-running it does not create duplicates (idempotent MERGE). |
| Data quality | Every rule in Section 7 runs on every load; failures go to a quarantine table and a DQ results table, never silently dropped. |
| Model | Beats the persistence baseline on MAE on a time-held-out test period. If it does not, the report says so and explains why (that is a valid finding). |
| Honesty | Leakage ablation reported: score with and without % Iron Concentrate. Test period touched exactly once. |
| Dashboard | Deployed at a public URL, reading from the serving database, with a replay control that steps through the test period hour by hour. |
| Engineering | CI (GitHub Actions) runs lint and tests on every push. Docker Compose brings up the local stack. README has an architecture diagram, a 2-minute demo video and metrics generated from files, not typed by hand. |

# 4. Dataset

| **Property** | **Detail** |
| --- | --- |
| Name / source | "Quality Prediction in a Mining Process" by Eduardo Magalhães Oliveira. Kaggle (edumagalhaes/quality-prediction-in-a-mining-process); mirrored on OpenML (ID 43311). |
| Licence | CC0 Public Domain (as listed on OpenML). Confirm on the Kaggle page and cite it in the README. |
| Period | March to September 2017, from a real flotation plant. |
| Sampling | Some columns every 20 seconds (process sensors), others hourly (lab quality). |
| Columns | Date; % Iron Feed, % Silica Feed (feed quality); Starch Flow, Amina Flow, Ore Pulp Flow, Ore Pulp pH, Ore Pulp Density (key process variables); Flotation Column 01–07 Air Flow and Level; % Iron Concentrate, % Silica Concentrate (lab). |
| Target | % Silica Concentrate (last column). |

## 4.1 Known traps (confirm each one during profiling, then document it)

Do not assume these; check them in the Week 1 profiling notebook and write down what you actually find. Each confirmed trap becomes a sentence in your README and an interview talking point.

- **Number format.** Values may use a comma as the decimal separator (for example 55,2). Parse explicitly; never let the CSV reader guess.

- **Timestamp resolution.** Check whether the date column carries only the hour, with many 20-second rows sharing one timestamp. If so, you cannot recover the exact 20-second order inside an hour from the timestamp alone, so aggregate to hourly features rather than inventing sub-hour times.

- **Hourly columns repeated or interpolated.** Lab and feed columns are hourly, so inside an hour they will be constant or interpolated. Treat them as hourly facts, not 20-second signals.

- **Gaps.** Check for missing hours or days and for hours with far fewer rows than expected. Record them in a gaps table; never interpolate across a long gap.

- **Leakage through % Iron Concentrate.** It comes from the same lab assay as the target and is highly correlated with it. The dataset author explicitly asks whether silica can be predicted without it. Exclude it from features and report the ablation.

- **Persistence is strong.** Published work on this problem reports that a naive "same as last hour" forecast is a hard baseline to beat. Your model must be compared against it.

- **Suspiciously high published scores.** Some public notebooks report very high R² on this data. Check whether they used random splits (rows from the same hour in both train and test) or the iron column. Don't compare your honest number with a leaky one.

# 5. Architecture

  Raw CSV (Kaggle)                                      [ingest]

        |

  BRONZE  Delta: raw rows as text + load metadata       [no business logic]

        |

  SILVER  Delta: typed, validated, deduplicated          [DQ rules -> quarantine]

        |          + sensor-health flags

  GOLD    Delta: hourly features, lab facts, star schema [aggregation, joins]

        |

  ML      LightGBM soft-sensor, MLflow tracking + registry

        |

  GOLD    fact_predictions (+ interval, SHAP top drivers)

        |

  SERVING PostgreSQL (star schema, read-only analyst role)

        |

  APPS    Streamlit dashboard   |   Power BI report   |   (later) Analyst Agent

## 5.1 Two run modes, one codebase

|  | **Local mode** | **Databricks mode** |
| --- | --- | --- |
| Purpose | Fast development loop, CI, reviewers can run it | Cloud execution, the version you demo |
| Compute | PySpark + delta-spark in Docker | Databricks Free Edition (serverless, usage-limited) |
| Storage | Delta tables on local disk | Unity Catalog tables / volumes |
| Orchestration | CLI: iop run --stage all | Databricks Job with one task per stage |
| Serving DB | PostgreSQL container | Hosted PostgreSQL (for example Supabase free tier) |

Stage code is plain Python modules that take a SparkSession and a config object, so the same functions run in both modes. Only the config (paths, catalog names, connection strings) differs.

# 6. Technology stack

| **Layer** | **Choice** | **Why this, not something else** | **JD line it proves** |
| --- | --- | --- | --- |
| Language | Python, SQL | Industry default; all transforms have a SQL-readable Gold layer | Python, SQL |
| Processing | PySpark | Same code runs locally and on Databricks; scale test in Section 12 | Spark, data engineering |
| Storage | Delta Lake | ACID MERGE for idempotent loads, schema enforcement, time travel | Data platforms, warehousing |
| Platform | Databricks Free Edition | Named in the JD; free | Databricks, cloud platforms |
| ML | LightGBM (primary), XGBoost (comparison), Ridge (baseline) | Strong on tabular data; quantile objective gives intervals | Machine learning |
| Explainability | SHAP | Operators need the "why", not just a number | Business problem-solving |
| Experiment tracking | MLflow | Built into Databricks; model registry | ML engineering |
| Serving | PostgreSQL | What dashboards and the later agent query; enforces keys and roles | Databases, SQL |
| Dashboard | Streamlit (deployed), Power BI (report file) | Streamlit is deployable for free; Power BI is named in the JD | Streamlit, Power BI |
| Quality | pytest, Delta CHECK constraints, custom DQ runner | No heavy framework needed at this size | Attention to detail |
| DevOps | Docker Compose, GitHub Actions, Makefile | One-command setup; tests on every push | Engineering practice |

# 7. Data model

## 7.1 Bronze

**bronze.flotation_raw**: every CSV column kept as a string, plus _source_file, _ingested_at, _row_number and _batch_id. Nothing is dropped or corrected here. Bronze is the audit trail.

## 7.2 Silver

- **silver.process_readings**: typed 20-second rows with canonical snake_case sensor columns, hour_ts, and a per-row dq_status.

- **silver.lab_results**: one row per hour with pct_iron_concentrate and pct_silica_concentrate (deduplicated from the repeated rows).

- **silver.feed_quality**: one row per hour with pct_iron_feed and pct_silica_feed.

- **silver.quarantine**: rejected rows with the rule that rejected them and the batch id.

- **silver.sensor_health**: per sensor per hour: flatline, out-of-range and spike flags.

## 7.3 Gold: star schema

| **Table** | **Grain** | **Key columns** |
| --- | --- | --- |
| fact_process_hourly | plant × hour | hour_key, plant_key; per sensor: mean, std, min, max, last-15-min mean, slope; row_count, completeness_pct |
| fact_lab_quality | plant × hour | hour_key, plant_key, pct_silica_concentrate, pct_iron_concentrate, is_off_spec |
| fact_predictions | plant × hour × model version | hour_key, plant_key, model_key, predicted_silica, p10, p90, top_driver_1..3, horizon_h |
| fact_dq_results | rule × batch | rule_id, batch_id, rows_checked, rows_failed, passed |
| fact_pipeline_runs | run × stage | run_id, stage, started_at, finished_at, rows_in, rows_out, status |
| dim_time | hour | hour_key, ts, date, hour, shift (A/B/C), day_of_week, week, month |
| dim_sensor | sensor | sensor_key, name, unit, group (feed / reagent / pulp / column), valid_min, valid_max |
| dim_plant | plant | plant_key, name, is_synthetic (true only for scale-test replicas) |
| dim_model | model version | model_key, mlflow_run_id, algorithm, trained_on, features_hash, test_mae |

**Shift definition.** The data does not include shifts. Define them yourself (for example three 8-hour shifts) in config and state that it is an assumption.

# 8. Data-quality rules

| **ID** | **Rule** | **Layer** | **On failure** |
| --- | --- | --- | --- |
| DQ01 | Required columns present; schema matches the contract | Bronze → Silver | Fail the run |
| DQ02 | Every numeric field parses after decimal-comma handling | Silver | Quarantine row |
| DQ03 | Timestamp parses and falls in the expected date range | Silver | Quarantine row |
| DQ04 | Exact duplicate rows removed; duplicate count logged | Silver | Log count |
| DQ05 | Physical ranges: percentages in [0, 100], pH in a plausible range, flows and levels ≥ 0 | Silver | Quarantine row |
| DQ06 | Rows per hour within an expected band (flags partial hours) | Gold | Set completeness_pct; exclude hours below threshold from training |
| DQ07 | Exactly one lab value per hour after deduplication | Silver | Fail the run if violated |
| DQ08 | Freshness and row-count reconciliation between layers | All | Fail the run if counts don't reconcile |
| DQ09 | Gold keys unique; every fact key exists in its dimension | Gold | Fail the run |

Thresholds live in config/dq.yaml, not in code. Every run writes to fact_dq_results, which feeds the Pipeline Health page.

# 9. Machine-learning design

## 9.1 Problem framing

**Nowcast:** estimate % silica for hour h using process data up to the end of hour h and lab results only up to hour h − L, where L is the lab delay (config, default 1 hour). The dataset does not state the real assay delay, so L is an explicit, documented assumption.

**Horizon study:** repeat the setup for predicting hour h + k (k = 1 to 4). This answers the dataset author's own question, "how many hours ahead can we predict?"

## 9.2 Features (all built only from information available at prediction time)

- Hourly aggregates of each process sensor: mean, std, min, max, last-15-minute mean, within-hour slope.

- Lagged sensor aggregates (1–3 hours back) to capture process residence time.

- Feed quality for the hour.

- Lagged lab silica at h − L, h − L − 1, … (the autoregressive signal).

- Calendar: hour of day, shift.

- **Excluded:** % Iron Concentrate at any lag at or after h − L + 1, and anything computed with future rows. A unit test checks that no feature timestamp is later than the prediction cut-off.

## 9.3 Validation

- Split by time only. Default: train on March–June, validate on July, hold out August–September as the test set. Adjust the boundaries after profiling if a gap sits on a boundary, and write down why.

- Tune with walk-forward (expanding-window) cross-validation inside train + validation.

- Touch the test set once, at the end. Log that run separately in MLflow.

- Never use a random split. Neighbouring hours are nearly identical, so a random split leaks.

## 9.4 Models and baselines

| **Model** | **Role** |
| --- | --- |
| Persistence (silica at h − L) | The benchmark to beat. Report skill = 1 − MAE_model / MAE_persistence. |
| Ridge regression | Linear baseline; shows how much non-linearity adds. |
| LightGBM | Primary model. Quantile objective (0.1 and 0.9) for an 80% interval. |
| XGBoost | Comparison only; keep whichever wins on validation. |

## 9.5 Metrics and reports

- MAE and RMSE on the test period; skill versus persistence.

- Off-spec detection: precision and recall for is_off_spec, where the threshold is the 75th percentile of training silica (a stated analysis choice, not a real plant specification).

- Interval coverage: share of test hours where the actual value falls inside p10–p90 (target about 80%).

- **Leakage ablation:** the same model with and without % Iron Concentrate, and with a random split versus a time split. Put the four numbers in one table. This is your strongest interview material.

- Horizon curve: MAE and skill for k = 0 to 4 hours ahead.

- SHAP: global importance plus per-hour top three drivers stored in fact_predictions.

All metrics are written to reports/metrics.json by code. The README table is generated from that file.

# 10. Sensor and process health (rule-based, not ML)

These run in Silver and need no training, so they don't add a second ML project.

- **Flatline:** a sensor with near-zero variance for N consecutive readings (stuck or frozen).

- **Out of range:** value outside dim_sensor valid_min / valid_max.

- **Spike:** rolling z-score above a threshold.

- **Quality control chart:** Shewhart limits on hourly lab silica, computed from the training period.

Hours with unhealthy key sensors get a reduced-confidence flag on the dashboard. That is how data quality and model trust connect.

# 11. Serving layer and dashboard

## 11.1 PostgreSQL serving schema

Gold dimensions and facts are exported to a **serving** schema with primary keys, foreign keys and indexes on hour_key. Create two roles: **app_rw** for the loader and **analyst_ro** (SELECT only) for the dashboard and the future Analyst Agent. Keep the DDL in database/schema.sql and write the load as an upsert.

## 11.2 Streamlit pages

| **Page** | **Content** |
| --- | --- |
| Quality Now | Latest hour: predicted silica with p10–p90 band, last lab value, off-spec status, confidence flag. |
| Trends | Predicted versus actual over time, off-spec hours shaded, persistence line for comparison; filters for date range and shift. |
| Drivers | Global SHAP importance; per-hour top drivers; reagent-dosing view (amine, starch) against silica. |
| Sensor Health | Heatmap of flags per sensor per day; list of current issues. |
| Pipeline & Model Health | Last run status per stage, row counts, DQ pass rate, freshness, rolling 7-day MAE. |

**Replay control:** a slider or play button that steps through the held-out test period hour by hour, showing only what the model would have known at that point. This makes the demo feel live without pretending there is a live feed.

**Power BI:** one report file (.pbix) on the same serving tables with a quality overview and a shift comparison page, with screenshots in the README. Power BI Desktop is free on Windows.

# 12. Orchestration, operations and the honest scale test

## 12.1 Job graph

ingest_bronze -> build_silver -> dq_checks -> build_gold -> train_or_load_model

     -> score_predictions -> export_serving -> refresh_reports

Each stage writes a row to fact_pipeline_runs. On Databricks this is a Job with one task per stage; locally it is iop run --stage all. Training runs on demand; scoring runs every time.

## 12.2 Model monitoring

Rolling MAE against incoming lab values, and population stability index (PSI) on the top five features against the training distribution. Show both on the Pipeline & Model Health page, with a documented retraining trigger.

## 12.3 Scale test

The real dataset is small enough for pandas, and an interviewer may ask why you used Spark. Answer it with evidence: generate N synthetic plant replicas (time-shifted, with added noise, is_synthetic = true in dim_plant), run the pipeline at 1×, 10× and 50×, and record runtime locally and on Databricks. Report it as a synthetic scale benchmark. Never mix synthetic rows into model evaluation.

# 13. Repository structure

industrial-operations-intelligence/

├── CLAUDE.md                 # rules for Claude Code (Section 15)

├── README.md                 # problem, diagram, demo video, generated metrics

├── Makefile                  # make setup | pipeline | test | dashboard

├── docker-compose.yml        # spark + postgres + streamlit

├── config/

│   ├── local.yaml  databricks.yaml  dq.yaml  features.yaml

├── src/iop/

│   ├── cli.py                # iop run --stage ...

│   ├── ingest/bronze.py

│   ├── transform/silver.py  gold.py  sensor_health.py

│   ├── quality/rules.py  runner.py

│   ├── ml/features.py  train.py  evaluate.py  score.py  ablation.py

│   └── serving/export.py

├── database/schema.sql  roles.sql

├── dashboard/app.py  pages/

├── powerbi/iop_report.pbix

├── notebooks/01_profiling.ipynb  02_eda.ipynb   # exploration only

├── databricks/job.json       # job definition

├── scripts/scale_test.py

├── reports/metrics.json  figures/

├── tests/                    # unit + small end-to-end on sample data

│   └── data/sample.csv

└── .github/workflows/ci.yml

# 14. Three-week build plan

| **Days** | **Deliverable** | **Acceptance check** |
| --- | --- | --- |
| 1 | Repo scaffold, Docker Compose, CI, CLAUDE.md. Databricks Free Edition workspace created; confirm it supports Jobs, MLflow and the SQL features you need. | make test passes on an empty test; CI green |
| 2–3 | Profiling notebook: confirm or reject every trap in Section 4.1; write the findings. | A findings list in the README draft |
| 4–5 | Bronze and Silver with DQ rules, quarantine and sensor health; unit tests per rule. | Row counts reconcile; each DQ rule has a failing and a passing test |
| 6–7 | Gold star schema; idempotent MERGE; fact_pipeline_runs. | Running twice gives identical Gold row counts |
| 8 | Run the same pipeline on Databricks as a Job. | Local and cloud Gold counts match |
| 9–11 | Features with leak test, baselines, LightGBM, walk-forward CV, MLflow. | Leak test passes; validation skill vs persistence reported |
| 12 | Test-set evaluation (once), leakage ablation table, horizon curve, SHAP, intervals. | reports/metrics.json generated by code |
| 13 | PostgreSQL serving schema, roles, upsert export. | analyst_ro cannot write (tested) |
| 14–16 | Streamlit dashboard, all five pages, replay control; deploy it. | Public URL works from a phone |
| 17 | Power BI report on the serving tables. | Screenshots in README |
| 18 | Monitoring (rolling MAE, PSI) and the scale test. | Scale table in README |
| 19–20 | README, architecture diagram, 2-minute demo video, resume bullets with real numbers. | A stranger can run it from the README |
| 21 | Buffer, and a rehearsal of Section 17 out loud. | You can explain every module without notes |

# 15. Building it with Claude Code

Claude Code can write most of this quickly. The risk is ending up with a repository you can't explain. The technical interview will probe exactly the parts you didn't write by hand. Use these rules.

## 15.1 CLAUDE.md (put this in the repo root)

# Project rules

- Goal: flotation-plant silica soft-sensor

  on a Delta Lake medallion pipeline.

- Stage code lives in src/iop, takes (spark, config),

  and must run locally and on Databricks.

- Never use random train/test splits. Time-based splits only.

- Never use pct_iron_concentrate as a feature

  except inside ml/ablation.py.

- No feature may use data later than the prediction cut-off;

  tests/test_leakage.py enforces it.

- Never type metric values into README or docs;

  generate them from reports/metrics.json.

- Every transform and DQ rule gets a unit test

  on tests/data/sample.csv.

- Thresholds and paths go in config/*.yaml, not in code.

- Ask before adding any new dependency.

- Small commits, one stage at a time;

  run make test before finishing a task.

## 15.2 How to work each phase

- Ask for a plan first ("plan the Silver stage and its tests; don't write code yet"), read it, and correct it.

- Have it write the tests before the implementation for DQ rules and the leakage check.

- After each stage, ask it to explain the code back to you, then explain it yourself without looking. If you can't, don't move on.

- Make at least one real design decision yourself per stage (for example lab delay L, off-spec threshold, how partial hours are handled) and record it in docs/decisions.md. These become interview answers.

- Check its numbers. If a metric looks too good, assume leakage until proven otherwise.

# 16. Stretch goals (only after Section 3 is fully met)

| **Stretch** | **Value** | **Effort** |
| --- | --- | --- |
| Enterprise Data Analyst Agent on the serving schema | Text-to-SQL agent with analyst_ro, query validation and a 50–100 question eval set. Turns this into a two-part system. | 1–2 weeks |
| Databricks certification | Data Engineer Associate or a free Databricks Academy badge gives the JD a certification line | 1 week of study |
| File-replay streaming | Spark Structured Streaming over hourly files dropped into a folder. Label it replay, not live. | 2–3 days |
| Azure deployment | Raw data in ADLS Gen2 with Azure for Students credit | 1–2 days |
| Conformal prediction intervals | Coverage guarantee instead of relying on quantile loss alone | 1–2 days |

# 17. Resume and interview

## 17.1 Resume entry (fill the brackets only with numbers from reports/metrics.json)

**Industrial Operations Intelligence Platform** | PySpark, Delta Lake, Databricks, SQL, PostgreSQL, LightGBM, MLflow, Streamlit

- Built a Bronze/Silver/Gold lakehouse on Databricks with PySpark and Delta Lake over [N]k rows of real flotation-plant data, resolving mixed 20-second and hourly sampling and enforcing [N] data-quality rules with quarantine and idempotent MERGE loads.

- Developed a LightGBM soft-sensor that estimates silica impurity before the lab assay, improving MAE by [X]% over a persistence baseline on a time-held-out test period; showed how a leaking lab variable and random splits inflate accuracy.

- Modelled a star schema served from PostgreSQL and shipped a Streamlit dashboard (quality nowcast with intervals, SHAP drivers, sensor and pipeline health), with CI and one-command Docker setup.

## 17.2 Questions to rehearse

| **Likely question** | **Answer skeleton** |
| --- | --- |
| Why Spark for a few hundred MB? | Same code runs on Databricks at plant-network scale; show the scale-test table; admit pandas would be fine for this single file. |
| Why Delta instead of Parquet? | ACID MERGE for idempotent reruns, schema enforcement, time travel for audits. |
| Why not a random split? | Adjacent hours are nearly identical, so random splits leak; show the ablation table. |
| Why exclude % Iron Concentrate? | Same lab assay as the target, so it isn't available when the prediction is needed; show the with/without numbers. |
| What if the model barely beats persistence? | Say so; explain where it helps (process changes, off-spec transitions) using the horizon curve and off-spec recall. |
| How would this run in a real Group plant? | Replace CSV ingest with historian or OPC-UA extracts, get the real lab delay and quality specification from engineers, and validate with operators before anyone acts on it. |
| What was the hardest data issue? | Pick one trap you actually confirmed in Section 4.1 and walk through how you found and handled it. |

# 18. Implementation rule

| **Non-negotiable** Only put components, technologies and numbers on the resume after they are implemented, tested and reproducible from the repository. Don't claim streaming, Azure, Airflow, Kafka, real-time, accuracy, latency or scale figures the project doesn't demonstrate. If you built it with Claude Code, you must still be able to explain and defend every part of it. |
| --- |

Industrial Operations Intelligence Platform v2  |