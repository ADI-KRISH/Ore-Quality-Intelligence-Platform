import numpy as np
import pandas as pd

from iop.ml.experiments import (
    add_engineered_features,
    choose_alpha,
    conformal_quantile_interval,
)


def test_choose_alpha_defaults_to_one_without_history():
    assert choose_alpha([]) == 1.0


def test_choose_alpha_zero_when_model_is_noise_and_one_when_perfect():
    rng = np.random.default_rng(0)
    last = rng.normal(size=200)
    true_delta = rng.normal(scale=0.3, size=200)
    y = last + true_delta
    assert choose_alpha([(y, last, rng.normal(size=200) * 5)]) <= 0.1
    assert choose_alpha([(y, last, true_delta)]) == 1.0


def test_engineered_features_use_only_lab_lags_and_current_sensors():
    pdf = pd.DataFrame(
        {
            "ore_pulp_flow_mean": [400.0, 0.0],
            "amina_flow_mean": [600.0, 600.0],
            "starch_flow_mean": [3000.0, 3000.0],
            "lab_silica_lag1": [2.0, 3.0],
            "lab_silica_lag2": [1.0, 3.0],
            "lab_silica_lag3": [3.0, 3.0],
        }
    )
    out = add_engineered_features(pdf)
    assert out.loc[0, "amina_per_pulp"] == 1.5
    assert np.isnan(out.loc[1, "amina_per_pulp"])  # zero pulp flow -> NaN, not inf
    assert out.loc[0, "lab_silica_trend"] == 1.0
    assert out.loc[0, "lab_silica_roll3_mean"] == 2.0


def test_conformal_interval_reaches_target_coverage_on_stationary_noise():
    rng = np.random.default_rng(1)
    n = 1500
    X = pd.DataFrame({"a": rng.normal(size=n), "b": rng.normal(size=n)})
    y = pd.Series(0.5 * X["a"] + rng.normal(scale=1.0, size=n))
    lo, hi = conformal_quantile_interval(X.iloc[:1000], y.iloc[:1000], X.iloc[1000:])
    cover = np.mean((y.iloc[1000:].to_numpy() >= lo) & (y.iloc[1000:].to_numpy() <= hi))
    assert 0.72 <= cover <= 0.90
