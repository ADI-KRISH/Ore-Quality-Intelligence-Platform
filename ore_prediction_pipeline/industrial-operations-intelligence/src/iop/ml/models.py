"""Model wrappers (spec 9.4): persistence baseline, Ridge, LightGBM
(point + quantile), XGBoost. Each fit_predict_* takes (train_X, train_y,
val_X) and returns predictions for val_X - fit on train, nothing else."""

from __future__ import annotations

import numpy as np
import pandas as pd

NON_FEATURE_COLUMNS = {"hour_ts", "plant_key", "target"}
SHIFT_CATEGORIES = ("A", "B", "C")


def prepare_matrix(pdf: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """One-hot encodes `shift` (with fixed categories, so a fold missing one
    shift still gets consistent columns) and splits into X, y."""
    df = pdf.copy()
    shift_dummies = pd.get_dummies(df["shift"], prefix="shift")
    for cat in SHIFT_CATEGORIES:
        col = f"shift_{cat}"
        if col not in shift_dummies.columns:
            shift_dummies[col] = False
    df = pd.concat([df.drop(columns=["shift"]), shift_dummies], axis=1)
    feature_cols = [c for c in df.columns if c not in NON_FEATURE_COLUMNS]
    return df[feature_cols], df["target"]


def fit_predict_persistence(val_X: pd.DataFrame) -> np.ndarray:
    """The benchmark: predict hour h's silica as whatever it was at h - L,
    the most recent lab reading actually available at prediction time."""
    return val_X["lab_silica_lag1"].to_numpy()


def fit_predict_ridge(train_X: pd.DataFrame, train_y: pd.Series, val_X: pd.DataFrame) -> np.ndarray:
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    pipe = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(), Ridge(alpha=1.0))
    pipe.fit(train_X, train_y)
    return pipe.predict(val_X)


def fit_predict_lightgbm(
    train_X: pd.DataFrame, train_y: pd.Series, val_X: pd.DataFrame, objective: str = "regression_l1", alpha: float | None = None
) -> np.ndarray:
    import lightgbm as lgb

    kwargs = {"objective": objective, "n_estimators": 300, "learning_rate": 0.05, "num_leaves": 31, "verbosity": -1}
    if alpha is not None:
        kwargs["alpha"] = alpha
    model = lgb.LGBMRegressor(**kwargs)
    model.fit(train_X, train_y)
    return model.predict(val_X)


def fit_predict_xgboost(train_X: pd.DataFrame, train_y: pd.Series, val_X: pd.DataFrame) -> np.ndarray:
    import xgboost as xgb

    model = xgb.XGBRegressor(
        objective="reg:absoluteerror", n_estimators=300, learning_rate=0.05, max_depth=6, verbosity=0
    )
    model.fit(train_X, train_y)
    return model.predict(val_X)


def mae(y_true, y_pred) -> float:
    return float(np.mean(np.abs(np.asarray(y_true) - np.asarray(y_pred))))


def skill_vs_persistence(model_mae: float, persistence_mae: float) -> float:
    return 1 - model_mae / persistence_mae


def interval_coverage(y_true, p10, p90) -> float:
    y_true = np.asarray(y_true)
    inside = (y_true >= np.asarray(p10)) & (y_true <= np.asarray(p90))
    return float(np.mean(inside))
