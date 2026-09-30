| # | Date | Decision | Options considered | Why this one | Stage |
|---|------|----------|--------------------|--------------|-------|
| D01 | 2026-09-30 | Lab delay L = 1 hour | 1 / 2 hours | Real assay delay unknown; 1 hour is the simplest stated assumption | ML |
| D02 | 2026-09-30 | Three 8-hour shifts: A 06–14, B 14–22, C 22–06 | Common plant pattern | Data has no shift info; this is an assumption | Gold |
| D03 | 2026-09-30 | Hours with less than 80% of expected rows are excluded from training | 50% / 80% / 100% | Partial hours give unreliable averages | Gold/DQ06 |
| D04 | 2026-09-30 | Off-spec = silica above the 75th percentile of training data | P75 / P90 | Analysis choice, not a real plant spec | ML |
| D05 | 2026-09-30 | Train Mar–Jun, validate Jul, test Aug–Sep | Adjust if a gap falls on a boundary | Time-based split avoids leakage | ML |
| D06 | 2026-09-30 | 320 hours (7.8%, silica-varying OR iron-varying - corrected from an initial 310 that only checked silica) where lab values vary within the hour are flagged as interpolated, excluded from training targets, evaluation and lag features | Last value / mean / quarantine | Varying values are likely interpolated, and interpolation leaks the next assay | Silver/ML |