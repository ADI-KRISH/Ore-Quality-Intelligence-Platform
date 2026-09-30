# Profiling findings (spec §4.1)

Fill from notebooks/01_profiling.ipynb. Write what you actually FOUND, with numbers.

| Trap | Confirmed? | Evidence (numbers, cell ref) | How the pipeline handles it |
|------|-----------|------------------------------|-----------------------------|
| Decimal comma number format | Yes | Raw values quoted as e.g. `"55,2"`, `"16,98"` throughout; confirmed on first 10 rows of the raw CSV. | Explicit comma-to-dot parsing in Silver; DQ02 quarantines rows that fail to parse. |
| Timestamp only at hour resolution | Yes | `date` column repeats one value for ~174-180 consecutive rows before changing (checked 2017-03-10 01:00-03:00: 174/180/45 rows). 4,097 distinct hours span 2017-03-10 to 2017-09-11. | No sub-hour timestamps invented; rows aggregated to `hour_ts`. |
| Hourly columns repeated / interpolated | Yes, and more nuanced than expected | Of 4,097 hours, 3,787 (92.4%) have exactly one distinct `% Silica Concentrate` value repeated across the hour, as expected for an hourly assay. But 310 hours (7.6%), scattered from 2017-03-12 to 2017-09-06 (not one contiguous block), have up to 180 distinct values in the hour instead of one. | D06: these 310 hours are flagged `lab_is_interpolated = true` in `silver.lab_results` (value stored as the hourly mean, for display only), logged under DQ10, and excluded from training targets, test evaluation, and any lagged-lab feature that would reference them (set to null). |
| Gaps (missing hours/days, partial hours) | Partially confirmed | First hour of data (2017-03-10 01:00) has only 174/180 rows (partial start). Full gap inventory (missing hours/days) still to be run over the complete date range. | Rows-per-hour below 80% completeness (D03) excluded from training via DQ06; long gaps never interpolated across. |
| Leakage via % Iron Concentrate (corr with target) | To confirm | Correlation not yet computed. | Excluded from all features except `ml/ablation.py` (CLAUDE.md rule); leak test enforces it. |
| Persistence baseline is strong | To confirm | MAE of lag-1/lag-2 persistence not yet computed. | Reported as the benchmark every model must beat (skill metric). |
| Published high scores use random split / iron column | To confirm | Not yet audited against public notebooks. | Time-based splits only (D05); ablation table reports random-split score separately as a cautionary number, never as the headline metric. |
