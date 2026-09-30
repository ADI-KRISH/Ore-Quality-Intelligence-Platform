"""Raw CSV header -> canonical snake_case column names (CLAUDE.md).

Delta/Parquet column names can't contain spaces, %, commas or parentheses, so
even Bronze renames columns to these canonical names - only the VALUES stay
untouched (raw strings) at that layer. Renaming a column is not "correcting"
data; the string "55,2" inside it is untouched until Silver parses it.
"""

from __future__ import annotations

RAW_TO_CANONICAL: dict[str, str] = {
    "date": "ts",
    "% Iron Feed": "pct_iron_feed",
    "% Silica Feed": "pct_silica_feed",
    "Starch Flow": "starch_flow",
    "Amina Flow": "amina_flow",
    "Ore Pulp Flow": "ore_pulp_flow",
    "Ore Pulp pH": "ore_pulp_ph",
    "Ore Pulp Density": "ore_pulp_density",
    "% Iron Concentrate": "pct_iron_concentrate",
    "% Silica Concentrate": "pct_silica_concentrate",
}
for _n in range(1, 8):
    RAW_TO_CANONICAL[f"Flotation Column 0{_n} Air Flow"] = f"col{_n:02d}_air_flow"
    RAW_TO_CANONICAL[f"Flotation Column 0{_n} Level"] = f"col{_n:02d}_level"

CANONICAL_COLUMNS = list(RAW_TO_CANONICAL.values())

# Columns that are numeric with a decimal-comma in the raw CSV (everything
# except the timestamp).
NUMERIC_COLUMNS = [c for c in CANONICAL_COLUMNS if c != "ts"]

PROCESS_SENSOR_COLUMNS = [
    "starch_flow",
    "amina_flow",
    "ore_pulp_flow",
    "ore_pulp_ph",
    "ore_pulp_density",
] + [f"col{n:02d}_air_flow" for n in range(1, 8)] + [f"col{n:02d}_level" for n in range(1, 8)]

LAB_COLUMNS = ["pct_iron_concentrate", "pct_silica_concentrate"]
FEED_COLUMNS = ["pct_iron_feed", "pct_silica_feed"]
