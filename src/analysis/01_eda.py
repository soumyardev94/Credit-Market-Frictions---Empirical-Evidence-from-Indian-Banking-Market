# src/analysis/01_eda.py
# Minimal, portfolio-friendly EDA on the final_panel.csv
# Outputs:
#   reports/tables/eda_summary.csv
#   reports/tables/missingness.csv
#   reports/tables/correlation_core.csv
#   reports/eda_notes.md

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "outputs" / "processed" / "final_panel.csv"
OUT_TBL = ROOT / "reports" / "tables"
OUT_MD = ROOT / "reports"
OUT_TBL.mkdir(parents=True, exist_ok=True)
OUT_MD.mkdir(parents=True, exist_ok=True)

# Core variables for EDA (edit if you want)
CORE_COLS = [
    "year",
    "nominal_gdp",
    "log_nominal_gdp",
    "nominal_gdp_growth",
    "bank_credit",
    "log_bank_credit",
    "log_bank_credit_growth",
    "crar",
    "capital_surplus_ratio",
    "net_npa_ratio",
    "change_in_rw",
    "leverage_ratiotrc_to_ta",
]


def main():
    if not DATA.exists():
        raise FileNotFoundError(f"Missing: {DATA}")

    df = pd.read_csv(DATA)

    # Keep only columns that exist (prevents crashes if some columns differ)
    cols = [c for c in CORE_COLS if c in df.columns]
    d = df[cols].copy()

    # Ensure year is numeric/int
    if "year" in d.columns:
        d["year"] = pd.to_numeric(d["year"], errors="coerce").astype("Int64")

    # Convert all non-year columns to numeric where possible
    for c in d.columns:
        if c != "year":
            d[c] = pd.to_numeric(d[c], errors="coerce")

    # Summary stats
    summary = d.describe().T
    summary.to_csv(OUT_TBL / "eda_summary.csv")

    # Missingness
    missing = pd.DataFrame({
        "missing_count": d.isna().sum(),
        "missing_pct": d.isna().mean(),
    }).sort_values("missing_pct", ascending=False)
    missing.to_csv(OUT_TBL / "missingness.csv", index=True)

    # Correlations (numeric only)
    numeric_cols = [c for c in d.columns if c != "year"]
    corr = d[numeric_cols].corr()
    corr.to_csv(OUT_TBL / "correlation_core.csv")

    # Notes markdown (human-readable)
    lines = ["# EDA Notes", ""]
    lines.append(f"- Dataset: `{DATA}`")
    lines.append(f"- Rows: **{df.shape[0]}**")
    lines.append(f"- Columns: **{df.shape[1]}**")
    if "year" in df.columns:
        yr = pd.to_numeric(df["year"], errors="coerce")
        lines.append(f"- Year range: **{int(yr.min())}–{int(yr.max())}**")
    lines.append("")
    lines.append("## Core columns included in EDA")
    lines.append(", ".join(cols))
    lines.append("")
    lines.append("## Top missingness (core columns)")
    top_missing = missing.head(10)
    for idx, row in top_missing.iterrows():
        lines.append(f"- `{idx}`: {int(row['missing_count'])} missing ({row['missing_pct']:.1%})")
    lines.append("")
    lines.append("## Output files")
    lines.append(f"- `{OUT_TBL / 'eda_summary.csv'}`")
    lines.append(f"- `{OUT_TBL / 'missingness.csv'}`")
    lines.append(f"- `{OUT_TBL / 'correlation_core.csv'}`")

    (OUT_MD / "eda_notes.md").write_text("\n".join(lines), encoding="utf-8")

    print("[OK] Wrote:", OUT_TBL / "eda_summary.csv")
    print("[OK] Wrote:", OUT_TBL / "missingness.csv")
    print("[OK] Wrote:", OUT_TBL / "correlation_core.csv")
    print("[OK] Wrote:", OUT_MD / "eda_notes.md")


if __name__ == "__main__":
    main()