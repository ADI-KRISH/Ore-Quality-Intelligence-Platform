---
name: leak-audit
description: Audit the feature and evaluation code for temporal leakage and target leakage. Use before any model training run and whenever a metric looks too good.
---

Audit for leakage. Don't fix anything yet; report first.

Check and report on each, with file:line references:
1. Any use of pct_iron_concentrate outside src/iop/ml/ablation.py.
2. Any feature whose source timestamp is later than the prediction cut-off (hour h end for process data; h − L for lab data; h + k for horizon runs).
3. Rolling/lag windows that are centred or use future rows (e.g. shift(-n), center=True, windows without proper ordering/partitioning).
4. Any random split, shuffle, or KFold without time ordering.
5. Scalers/encoders/imputers fit on data that includes validation or test rows.
6. Any code path that reads the test period outside `iop evaluate --final`.
7. Hours with low completeness_pct leaking into training.
8. Synthetic plant rows reaching training/evaluation.

Then run `pytest tests/test_leakage.py -v` and show the output. Summarise as PASS / RISK / FAIL per item.
