"""Validation-only model experiments (D07: predict the change in silica).

Everything here reads the cached train+val feature table written by
`iop.ml.train._load_trainval_features`, which filters out the test period in
Spark before collecting - so Aug-Sep is structurally unreachable from this
module. Nothing here is tuned on or evaluated against test data.

Folds: walk-forward from May (train Mar..month-1, validate month). April is
reported separately (only ~205 training hours, too small to average in).

Every model predicts delta = target - lab_silica_lag1 and adds the last lab
value back, so MAE is on the silica level and directly comparable to
persistence (= "predict zero change").
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from iop.ml.models import (
    fit_predict_lightgbm,
    fit_predict_persistence,
    fit_predict_ridge,
    interval_coverage,
    mae,
    prepare_matrix,
    skill_vs_persistence,
)
from iop.ml.splits import walk_forward_folds

PLS_COMPONENTS = range(2, 11)
ALPHA_GRID = np.round(np.linspace(0.0, 1.0, 21), 2)
TARGET_COVERAGE = 0.80


def add_engineered_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """Step (d): reagent ratios and recent lab-silica trend. Sensor lags 1-3 h
    already exist upstream (features.yaml). The lab trend uses only lab lags
    (all available at prediction time, nulled upstream when interpolated).
    Window is the 3 available lab lags; longer windows would need more lags
    upstream and cost training rows (each extra lag nulls more hours)."""
    df = pdf.copy()
    pulp = df["ore_pulp_flow_mean"].replace(0, np.nan)
    df["amina_per_pulp"] = df["amina_flow_mean"] / pulp
    df["starch_per_pulp"] = df["starch_flow_mean"] / pulp
    df["amina_per_starch"] = df["amina_flow_mean"] / df["starch_flow_mean"].replace(0, np.nan)
    lab = df[["lab_silica_lag1", "lab_silica_lag2", "lab_silica_lag3"]]
    df["lab_silica_roll3_mean"] = lab.mean(axis=1)
    df["lab_silica_roll3_std"] = lab.std(axis=1)
    df["lab_silica_trend"] = df["lab_silica_lag1"] - df["lab_silica_lag2"]
    return df


def _split_fold(pdf: pd.DataFrame, fold: dict):
    ts = pdf["hour_ts"]
    train = pdf[(ts >= fold["train_start"]) & (ts < fold["train_end"])]
    train = train[train["lab_silica_lag1"].notna()]
    val = pdf[(ts >= fold["val_start"]) & (ts < fold["val_end"])]
    val = val[val["lab_silica_lag1"].notna()]
    return train, val


# ---- model fitters: (train_X, train_delta, val_X) -> predicted delta ----------


def fit_predict_pls(train_X, train_y, val_X, n_components: int | None = None) -> np.ndarray:
    """PLS on scaled features. n_components chosen by a forward-chaining CV
    inside the training window only (never looks at the validation fold)."""
    from sklearn.cross_decomposition import PLSRegression
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    def make(k):
        return make_pipeline(
            SimpleImputer(strategy="median"), StandardScaler(), PLSRegression(n_components=k)
        )

    if n_components is None:
        n_components = select_pls_components(train_X, train_y, make)
    model = make(n_components)
    model.fit(train_X, train_y)
    return np.asarray(model.predict(val_X)).ravel()


def select_pls_components(train_X, train_y, make) -> int:
    n = len(train_X)
    # 4 expanding-window splits inside train; first 40% is always train-only.
    cuts = [int(n * f) for f in (0.4, 0.55, 0.7, 0.85, 1.0)]
    best_k, best_err = PLS_COMPONENTS[0], np.inf
    for k in PLS_COMPONENTS:
        errs = []
        for lo, hi in zip(cuts[:-1], cuts[1:], strict=True):
            m = make(k).fit(train_X.iloc[:lo], train_y.iloc[:lo])
            pred = np.asarray(m.predict(train_X.iloc[lo:hi])).ravel()
            errs.append(mae(train_y.iloc[lo:hi], pred))
        err = float(np.mean(errs))
        if err < best_err:
            best_k, best_err = k, err
    return best_k


def fit_predict_lgbm_regularised(train_X, train_y, val_X) -> np.ndarray:
    """Step (c): small trees, large leaves, strong L1/L2, subsampling and
    early stopping on the chronologically last 20% of the TRAIN window."""
    import lightgbm as lgb

    cut = int(len(train_X) * 0.8)
    model = lgb.LGBMRegressor(
        objective="regression_l1",
        n_estimators=1000,
        learning_rate=0.03,
        num_leaves=7,
        min_child_samples=40,
        subsample=0.7,
        subsample_freq=1,
        colsample_bytree=0.6,
        reg_alpha=1.0,
        reg_lambda=5.0,
        verbosity=-1,
    )
    model.fit(
        train_X.iloc[:cut],
        train_y.iloc[:cut],
        eval_set=[(train_X.iloc[cut:], train_y.iloc[cut:])],
        eval_metric="l1",
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )
    return model.predict(val_X)


def fit_predict_sarimax(train_df, val_df, n_exog: int = 3) -> np.ndarray:
    """Step (e): AR(1) on the hourly delta series with the n most-correlated
    sensors (picked on train only) as exogenous inputs. Parameters are fit on
    the train window; the fitted model is then applied to train+val and
    ONE-STEP-AHEAD predictions are read off for the val hours. A one-step
    prediction for hour h uses delta up to h-1 (lab at h-1 is available under
    L=1) and exogenous inputs at h, so nothing from the future is used."""
    from statsmodels.tsa.statespace.sarimax import SARIMAX

    sensor_cols = [c for c in train_df.columns if c.endswith("_mean") and "lag" not in c]
    delta_train = train_df["target"] - train_df["lab_silica_lag1"]
    corr = train_df[sensor_cols].apply(lambda s: s.corr(delta_train)).abs().dropna()
    exog_cols = list(corr.sort_values(ascending=False).index[:n_exog])

    full = pd.concat([train_df, val_df])
    idx = pd.to_datetime(full["hour_ts"])
    y = pd.Series((full["target"] - full["lab_silica_lag1"]).to_numpy(), index=idx)
    X = full[exog_cols].copy()
    X.index = idx
    hourly = pd.date_range(idx.min(), idx.max(), freq="h")
    y, X = y.reindex(hourly), X.reindex(hourly)
    # Standardise exog on train stats; fill gaps with the train mean (= 0 after scaling).
    mu, sd = train_df[exog_cols].mean(), train_df[exog_cols].std().replace(0, 1)
    X = ((X - mu) / sd).fillna(0.0)

    # The val deltas must not influence the fit or the AR state before they are
    # "observed": fit on train rows only, then filter over all rows.
    n_train_end = pd.to_datetime(train_df["hour_ts"]).max()
    y_fit = y[y.index <= n_train_end]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fit = SARIMAX(y_fit, exog=X.loc[y_fit.index], order=(1, 0, 0), trend="c").fit(
            disp=False, maxiter=200
        )
        applied = fit.apply(y, exog=X)
        pred = applied.get_prediction().predicted_mean
    val_idx = pd.to_datetime(val_df["hour_ts"])
    return pred.reindex(val_idx).to_numpy()


def conformal_quantile_interval(train_X, train_y, val_X) -> tuple[np.ndarray, np.ndarray]:
    """Step (f): LightGBM quantile p10/p90 on the first 80% of train, widened
    by a split-conformal (CQR) correction computed on the last 20% so
    coverage lands near 80%. Returns delta-space bounds."""
    cut = int(len(train_X) * 0.8)
    fit_X, fit_y = train_X.iloc[:cut], train_y.iloc[:cut]
    cal_X, cal_y = train_X.iloc[cut:], train_y.iloc[cut:]
    both = pd.concat([cal_X, val_X])
    lo = fit_predict_lightgbm(fit_X, fit_y, both, objective="quantile", alpha=0.1)
    hi = fit_predict_lightgbm(fit_X, fit_y, both, objective="quantile", alpha=0.9)
    lo_cal, hi_cal = lo[: len(cal_X)], hi[: len(cal_X)]
    scores = np.maximum(lo_cal - cal_y.to_numpy(), cal_y.to_numpy() - hi_cal)
    n = len(scores)
    q = np.quantile(scores, min(1.0, np.ceil((n + 1) * TARGET_COVERAGE) / n))
    return lo[len(cal_X) :] - q, hi[len(cal_X) :] + q


# ---- blend --------------------------------------------------------------------


def choose_alpha(history: list[tuple[np.ndarray, np.ndarray, np.ndarray]]) -> float:
    """alpha minimising MAE of  last + alpha * predicted_delta  over the
    EARLIER folds' out-of-sample predictions (never the fold being scored).
    history items: (y_true, last_lab, predicted_delta). No history -> 1.0."""
    if not history:
        return 1.0
    y = np.concatenate([h[0] for h in history])
    last = np.concatenate([h[1] for h in history])
    d = np.concatenate([h[2] for h in history])
    errs = [mae(y, last + a * d) for a in ALPHA_GRID]
    return float(ALPHA_GRID[int(np.argmin(errs))])


# ---- driver -------------------------------------------------------------------

MODELS = [
    "persistence",
    "ridge",
    "lightgbm_default",
    "pls",
    "lightgbm_reg",
    "lightgbm_reg+feats",
    "pls+feats",
    "sarimax",
]


def run_experiments(pdf: pd.DataFrame, split_cfg) -> dict:
    folds = walk_forward_folds(split_cfg.train_start, split_cfg.val_start, split_cfg.val_end)
    pdf_feats = add_engineered_features(pdf)
    results = []
    blend_history: dict[str, list] = {"pls": [], "lightgbm_reg": [], "lightgbm_reg+feats": []}

    for fold in folds:  # fold 0 is April: reported separately, but feeds alpha history
        train, val = _split_fold(pdf, fold)
        train_f, val_f = _split_fold(pdf_feats, fold)
        train_X, train_y = prepare_matrix(train)
        val_X, val_y = prepare_matrix(val)
        train_Xf, _ = prepare_matrix(train_f)
        val_Xf, _ = prepare_matrix(val_f)
        base_tr = train_X["lab_silica_lag1"]
        d_train = train_y - base_tr
        last = val_X["lab_silica_lag1"].to_numpy()

        deltas = {
            "ridge": fit_predict_ridge(train_X, d_train, val_X),
            "lightgbm_default": fit_predict_lightgbm(
                train_X, d_train, val_X, objective="regression_l1"
            ),
            "pls": fit_predict_pls(train_X, d_train, val_X),
            "lightgbm_reg": fit_predict_lgbm_regularised(train_X, d_train, val_X),
            "lightgbm_reg+feats": fit_predict_lgbm_regularised(train_Xf, d_train, val_Xf),
            "pls+feats": fit_predict_pls(train_Xf, d_train, val_Xf),
            "sarimax": fit_predict_sarimax(train, val),
        }
        # SARIMAX can return NaN for hours it cannot score; fall back to "no change".
        deltas["sarimax"] = np.nan_to_num(deltas["sarimax"], nan=0.0)

        preds = {"persistence": fit_predict_persistence(val_X)}
        for name, d in deltas.items():
            preds[name] = last + d
        # (b) blends use alpha from EARLIER folds only
        for name in list(blend_history):
            alpha = choose_alpha(blend_history[name])
            preds[f"blend[{name}]"] = last + alpha * deltas[name]
            preds[f"blend[{name}]_alpha"] = alpha
            blend_history[name].append((val_y.to_numpy(), last, deltas[name]))

        lo_d, hi_d = conformal_quantile_interval(train_X, d_train, val_X)
        coverage_raw = None
        raw_lo = fit_predict_lightgbm(train_X, d_train, val_X, objective="quantile", alpha=0.1)
        raw_hi = fit_predict_lightgbm(train_X, d_train, val_X, objective="quantile", alpha=0.9)
        coverage_raw = interval_coverage(val_y, last + raw_lo, last + raw_hi)
        coverage_cal = interval_coverage(val_y, last + lo_d, last + hi_d)

        p_mae = mae(val_y, preds["persistence"])
        row = {
            "val_month": fold["val_start"],
            "n_train": len(train),
            "n_val": len(val),
            "persistence_mae": p_mae,
            "mae": {},
            "skill": {},
            "alpha": {},
            "coverage_raw": coverage_raw,
            "coverage_conformal": coverage_cal,
        }
        for name, p in preds.items():
            if name.endswith("_alpha"):
                row["alpha"][name[: -len("_alpha")]] = p
                continue
            m = mae(val_y, p)
            row["mae"][name] = m
            row["skill"][name] = skill_vs_persistence(m, p_mae)
        results.append(row)

    return {"april": results[0], "walk_forward_from_may": results[1:], "all": results}


def summarise(res: dict) -> pd.DataFrame:
    """Average skill over May-Jul plus each month, one row per model."""
    rows = res["walk_forward_from_may"]
    names = list(rows[0]["skill"])
    table = pd.DataFrame({r["val_month"][:7]: pd.Series(r["skill"]) for r in rows}).loc[names]
    table["avg_skill_May-Jul"] = table.mean(axis=1)
    table["apr_skill"] = pd.Series(res["april"]["skill"])
    return table


def main(cache: str = "data/cache/trainval_features.parquet", config: str = "config/local.yaml"):
    import mlflow

    from iop.config import load_config

    cfg = load_config(config)
    pdf = pd.read_parquet(cache)
    res = run_experiments(pdf, cfg.ml.split)
    table = summarise(res)

    pd.set_option("display.width", 200)
    print("\nSkill vs persistence (positive = better), delta target, validation only")
    print(table.round(3).to_string())
    print(
        "\nPersistence MAE by month:",
        {r["val_month"][:7]: round(r["persistence_mae"], 4) for r in res["all"]},
    )
    print("Blend alphas:", {r["val_month"][:7]: r["alpha"] for r in res["all"]})
    print(
        "p10-p90 coverage raw -> conformal:",
        {
            r["val_month"][:7]: (round(r["coverage_raw"], 3), round(r["coverage_conformal"], 3))
            for r in res["all"]
        },
    )

    mlflow.set_tracking_uri(cfg.mlflow.tracking_uri)
    mlflow.set_experiment(cfg.mlflow.experiment_name)
    for r in res["all"]:
        for name in r["mae"]:
            with mlflow.start_run(run_name=f"exp_{name}_{r['val_month'][:7]}"):
                mlflow.log_params(
                    {
                        "model": name,
                        "target_mode": "delta",
                        "val_month": r["val_month"],
                        "n_train": r["n_train"],
                        "n_val": r["n_val"],
                        "stage": "validation_experiments",
                    }
                )
                mlflow.log_metrics(
                    {"mae": r["mae"][name], "skill_vs_persistence": r["skill"][name]}
                )
    out = Path("reports")
    out.mkdir(exist_ok=True)
    (out / "experiments_validation.json").write_text(json.dumps(res, indent=2, default=float))
    return res


if __name__ == "__main__":
    main()
