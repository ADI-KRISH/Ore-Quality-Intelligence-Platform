"""Stage: train_or_load_model (spec 9.3-9.4). Walk-forward CV on train+val
ONLY - the test period is never read here, not even filtered out after
loading; the Spark filter below excludes it before a single row is collected.
"""

from __future__ import annotations

from datetime import datetime

import mlflow
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from iop.config import Config
from iop.delta_io import gold_location, read_table, silver_location
from iop.ml.features import build_features, load_features_config
from iop.ml.models import (
    fit_predict_lightgbm,
    fit_predict_persistence,
    fit_predict_ridge,
    fit_predict_xgboost,
    interval_coverage,
    mae,
    prepare_matrix,
    skill_vs_persistence,
)
from iop.ml.splits import walk_forward_folds
from iop.models import StageResult
from iop.quality.rules import load_dq_config

# Skill this high on a dataset whose own author calls persistence "a hard
# baseline to beat" is a leakage smell, not a win (CLAUDE.md: "if a metric
# looks too good, assume leakage"). This doesn't fail the stage - only the
# person reading the printed table can judge that - it just makes the
# warning impossible to miss.
SUSPICIOUS_SKILL_THRESHOLD = 0.5


def _slice(pdf: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    return pdf[(pdf["hour_ts"] >= start) & (pdf["hour_ts"] < end)]


def _load_trainval_features(spark: SparkSession, cfg: Config) -> pd.DataFrame:
    dq_cfg = load_dq_config()
    features_cfg = load_features_config()
    min_completeness = dq_cfg["hour_completeness"]["min_completeness_pct"]

    fact_process_hourly = read_table(spark, cfg, gold_location(cfg, "fact_process_hourly"))
    fact_lab_quality = read_table(spark, cfg, gold_location(cfg, "fact_lab_quality"))
    feed_quality = read_table(spark, cfg, silver_location(cfg, "feed_quality"))

    features = build_features(
        fact_process_hourly,
        feed_quality,
        fact_lab_quality,
        ml_cfg=cfg.ml,
        horizon=0,
        min_completeness_pct=min_completeness,
        features_cfg=features_cfg,
    )
    # Never touch the test period: filtered out here, in Spark, before
    # anything is collected to the driver.
    trainval = features.filter(
        (F.col("hour_ts") >= cfg.ml.split.train_start) & (F.col("hour_ts") < cfg.ml.split.test_start)
    )
    pdf = trainval.toPandas()
    pdf["hour_ts"] = pdf["hour_ts"].astype(str)
    return pdf


def _run_fold(pdf: pd.DataFrame, fold: dict, target_mode: str = "level") -> dict:
    """target_mode "delta": models learn the CHANGE since the last known lab
    value (target - lab_silica_lag1) and that value is added back before
    scoring, so MAE is on the silica level either way and directly comparable
    to persistence (which is exactly "predict zero change")."""
    train_pdf = _slice(pdf, fold["train_start"], fold["train_end"])
    if target_mode == "delta":
        # No last known value -> no defined change to learn from.
        train_pdf = train_pdf[train_pdf["lab_silica_lag1"].notna()]
    val_pdf = _slice(pdf, fold["val_start"], fold["val_end"])

    # The persistence baseline needs lab_silica_lag1 (h - L); D06 nulls it
    # for any hour whose h - L reading looked interpolated. Every model is
    # compared on the SAME rows, so this scopes the whole fold's evaluation
    # set, not just persistence's - one NaN in any model's MAE would
    # otherwise propagate to a NaN skill for every model (numpy's mean of an
    # array containing NaN is NaN), which is what shipped first.
    val_pdf = val_pdf[val_pdf["lab_silica_lag1"].notna()]

    train_X, train_y = prepare_matrix(train_pdf)
    val_X, val_y = prepare_matrix(val_pdf)

    base = 0.0
    if target_mode == "delta":
        train_y = train_y - train_X["lab_silica_lag1"]
        base = val_X["lab_silica_lag1"].to_numpy()

    predictions = {
        "persistence": fit_predict_persistence(val_X),
        "ridge": base + fit_predict_ridge(train_X, train_y, val_X),
        "lightgbm": base + fit_predict_lightgbm(train_X, train_y, val_X, objective="regression_l1"),
        "xgboost": base + fit_predict_xgboost(train_X, train_y, val_X),
    }
    p10 = base + fit_predict_lightgbm(train_X, train_y, val_X, objective="quantile", alpha=0.1)
    p90 = base + fit_predict_lightgbm(train_X, train_y, val_X, objective="quantile", alpha=0.9)

    persistence_mae = mae(val_y, predictions["persistence"])
    fold_result = {
        "fold": fold,
        "n_train": len(train_pdf),
        "n_val": len(val_pdf),
        "mae": {},
        "skill": {},
    }
    for name, preds in predictions.items():
        m = mae(val_y, preds)
        fold_result["mae"][name] = m
        fold_result["skill"][name] = skill_vs_persistence(m, persistence_mae)
    fold_result["p10_p90_coverage"] = interval_coverage(val_y, p10, p90)
    return fold_result


def _log_to_mlflow(cfg: Config, fold_results: list[dict], target_mode: str) -> None:
    mlflow.set_tracking_uri(cfg.mlflow.tracking_uri)
    mlflow.set_experiment(cfg.mlflow.experiment_name)
    for i, fold_result in enumerate(fold_results):
        for model_name in fold_result["mae"]:
            with mlflow.start_run(run_name=f"{model_name}_{target_mode}_fold{i}"):
                mlflow.log_params(
                    {
                        "model": model_name,
                        "target_mode": target_mode,
                        "fold": i,
                        "train_start": fold_result["fold"]["train_start"],
                        "train_end": fold_result["fold"]["train_end"],
                        "val_start": fold_result["fold"]["val_start"],
                        "val_end": fold_result["fold"]["val_end"],
                        "n_train": fold_result["n_train"],
                        "n_val": fold_result["n_val"],
                    }
                )
                mlflow.log_metrics(
                    {"mae": fold_result["mae"][model_name], "skill_vs_persistence": fold_result["skill"][model_name]}
                )
                if model_name == "lightgbm":
                    mlflow.log_metric("p10_p90_coverage", fold_result["p10_p90_coverage"])


def _print_comparison_table(fold_results: list[dict], target_mode: str) -> None:
    models = list(fold_results[0]["mae"].keys())
    cv_mae = {m: sum(f["mae"][m] for f in fold_results) / len(fold_results) for m in models}
    cv_skill = {m: sum(f["skill"][m] for f in fold_results) / len(fold_results) for m in models}
    last = fold_results[-1]

    print(f"\nValidation comparison, target={target_mode} (last fold: train Mar-Jun, val Jul)")
    for i, f in enumerate(fold_results):
        skills = "  ".join(f"{m}={f['skill'][m]:+.3f}" for m in models if m != "persistence")
        print(
            f"  fold{i} val {f['fold']['val_start']} n_train={f['n_train']:<5} "
            f"n_val={f['n_val']:<5} "
            f"persistence_MAE={f['mae']['persistence']:.4f}  {skills}"
        )
    print(f"{'model':<12} {'val_MAE':>10} {'val_skill':>10} {'cv_MAE(avg)':>12} {'cv_skill(avg)':>14}")
    for m in models:
        flag = " <-- SUSPICIOUS" if last["skill"][m] > SUSPICIOUS_SKILL_THRESHOLD else ""
        print(
            f"{m:<12} {last['mae'][m]:>10.4f} {last['skill'][m]:>10.3f} "
            f"{cv_mae[m]:>12.4f} {cv_skill[m]:>14.3f}{flag}"
        )
    print(f"\nLightGBM p10-p90 coverage on last fold's val set: {last['p10_p90_coverage']:.3f} (target ~0.80)")


def run(spark: SparkSession, cfg: Config) -> StageResult:
    started = datetime.utcnow()

    pdf = _load_trainval_features(spark, cfg)
    folds = walk_forward_folds(cfg.ml.split.train_start, cfg.ml.split.val_start, cfg.ml.split.val_end)
    results_by_mode = {}
    for target_mode in ("level", "delta"):
        fold_results = [_run_fold(pdf, fold, target_mode) for fold in folds]
        _log_to_mlflow(cfg, fold_results, target_mode)
        _print_comparison_table(fold_results, target_mode)
        results_by_mode[target_mode] = fold_results

    fold_results = results_by_mode["level"]
    last = fold_results[-1]
    suspicious = [m for m, s in last["skill"].items() if m != "persistence" and s > SUSPICIOUS_SKILL_THRESHOLD]

    return StageResult(
        stage="train_or_load_model",
        rows_in=len(pdf),
        rows_out=sum(f["n_val"] for f in fold_results),
        status="success",
        started_at=started,
        finished_at=datetime.utcnow(),
        extra={
            "folds": len(folds),
            "last_fold_mae": last["mae"],
            "last_fold_skill": last["skill"],
            "suspicious_models": suspicious,
            "last_fold_skill_delta": results_by_mode["delta"][-1]["skill"],
        },
    )
