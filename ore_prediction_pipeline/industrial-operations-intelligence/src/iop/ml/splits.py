"""Walk-forward (expanding-window) CV folds, train+val only (spec 9.3).
The test period is not even a parameter here - it structurally cannot leak
into tuning."""

from __future__ import annotations

import pandas as pd


def walk_forward_folds(train_start: str, val_start: str, val_end: str) -> list[dict]:
    """One fold per month from train_start's second month through val_end's
    month: fold i trains on [train_start, month_i) and validates on
    [month_i, month_i+1). Half-open ranges throughout, so a fold's val month
    is never touched again by any earlier fold's train range.

    The LAST fold's train range is exactly [train_start, val_start) and its
    val range is exactly [val_start, val_end)'s month - the split spec 9.3
    states directly. Earlier folds are the expanding-window CV around it.
    """
    first_month = pd.Timestamp(train_start).replace(day=1)
    last_val_month = pd.Timestamp(val_end).replace(day=1)

    months = []
    m = first_month
    while m <= last_val_month:
        months.append(m)
        m = m + pd.DateOffset(months=1)
    months.append(m)  # sentinel: one past the last val month, as its upper bound

    folds = []
    for i in range(1, len(months) - 1):
        folds.append(
            {
                "train_start": months[0].date().isoformat(),
                "train_end": months[i].date().isoformat(),
                "val_start": months[i].date().isoformat(),
                "val_end": months[i + 1].date().isoformat(),
            }
        )
    return folds
