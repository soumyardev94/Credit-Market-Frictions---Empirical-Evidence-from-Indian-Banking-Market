# src/pipeline/etl_pipeline.py
# Single consolidated ETL: validate -> standardize -> merge -> consolidate -> finalize -> QA -> data dictionary

from __future__ import annotations

from pathlib import Path
import re
import sys
import pandas as pd


# -----------------------------
# Repo root discovery
# -----------------------------
def find_repo_root(start: Path) -> Path:
    """
    Walk upwards until we find a folder that looks like the repo root.
    We assume repo root contains a 'data' directory.
    """
    cur = start.resolve()
    for _ in range(8):
        if (cur / "data").exists():
            return cur
        cur = cur.parent
    # fallback: assume 2 levels up from src/pipeline
    return start.resolve().parents[2]


# -----------------------------
# Config
# -----------------------------
HERE = Path(__file__).resolve()
ROOT = find_repo_root(HERE.parent)

DATA_DIR = ROOT / "data" / "source_clean"
OUT_DIR = ROOT / "outputs"
PROC_DIR = OUT_DIR / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

FILES = {
    "Business Cycle Link": DATA_DIR / "India_GDP_Credit.xlsx",
    "Leverage Analysis": DATA_DIR / "LeverageData.xlsx",
    "Balance Sheet Analysis": DATA_DIR / "CapitalAdequacy_Analysis.xlsx",
}

SHEETS = {
    "Business Cycle Link": "Sheet1",
    "Leverage Analysis": "Data",
    "Balance Sheet Analysis": "Sheet1",
}


# -----------------------------
# Step 1: Validation (trust layer)
# -----------------------------
def inspect_excel(path: Path) -> dict:
    xls = pd.ExcelFile(path)
    info = {
        "path": str(path),
        "sheets": xls.sheet_names,
        "n_sheets": len(xls.sheet_names),
    }
    df0 = pd.read_excel(path, sheet_name=xls.sheet_names[0])
    info["first_sheet"] = xls.sheet_names[0]
    info["first_sheet_rows"] = int(df0.shape[0])
    info["first_sheet_cols"] = int(df0.shape[1])
    info["first_sheet_columns"] = [str(c) for c in df0.columns]
    return info


def run_validation() -> None:
    rows = []
    md_lines = ["# Data Validation Report\n"]

    for name, path in FILES.items():
        if not path.exists():
            rows.append({"dataset": name, "status": "MISSING", "path": str(path)})
            md_lines.append(f"## {name}\n- ❌ Missing file: `{path}`\n")
            continue

        info = inspect_excel(path)
        rows.append(
            {
                "dataset": name,
                "status": "OK",
                "path": info["path"],
                "n_sheets": info["n_sheets"],
                "first_sheet": info["first_sheet"],
                "first_sheet_rows": info["first_sheet_rows"],
                "first_sheet_cols": info["first_sheet_cols"],
            }
        )

        md_lines.append(f"## {name}\n")
        md_lines.append(f"- ✅ File found: `{path}`")
        md_lines.append(f"- Sheets ({info['n_sheets']}): {', '.join(info['sheets'])}")
        md_lines.append(
            f"- First sheet preview: `{info['first_sheet']}` "
            f"({info['first_sheet_rows']} rows × {info['first_sheet_cols']} cols)"
        )
        md_lines.append(f"- Columns: {', '.join(info['first_sheet_columns'])}\n")

    summary = pd.DataFrame(rows)
    summary.to_csv(OUT_DIR / "data_validation_summary.csv", index=False)
    (OUT_DIR / "data_validation_report.md").write_text("\n".join(md_lines), encoding="utf-8")

    print("[OK] Validation report:", OUT_DIR / "data_validation_report.md")
    print("[OK] Validation summary:", OUT_DIR / "data_validation_summary.csv")


# -----------------------------
# Step 2: Extract + standardize
# -----------------------------
def clean_colname(col: str) -> str:
    col = str(col).strip()
    col = col.replace("–", "-").replace("—", "-")
    col = col.replace("/", "_to_")
    col = re.sub(r"[()]", "", col)
    col = re.sub(r"[%]", "pct", col)
    col = re.sub(r"\s+", "_", col)
    col = re.sub(r"[-]+", "_", col)
    col = re.sub(r"__+", "_", col)
    return col.lower().strip("_")


def parse_year(value) -> int | None:
    if pd.isna(value):
        return None
    s = str(value)
    m = re.search(r"(19\d{2}|20\d{2})", s)
    return int(m.group(1)) if m else None


def standardize_business_cycle() -> pd.DataFrame:
    path = FILES["Business Cycle Link"]
    df = pd.read_excel(path, sheet_name=SHEETS["Business Cycle Link"])
    df.columns = [clean_colname(c) for c in df.columns]

    rename_map = {
        "year": "year",
        "gdp": "gdp_level",
        "log_gdp": "gdp_log",
        "log_gdp_growth": "gdp_log_growth",
        "bank_credit": "bank_credit_level",
        "log_bank_credit": "bank_credit_log",
        "log_credit_growth": "bank_credit_log_growth",
    }
    df = df.rename(columns=rename_map)
    df["year"] = df["year"].astype(int)

    keep = list(rename_map.values())
    return df[keep].sort_values("year").reset_index(drop=True)


def standardize_leverage() -> pd.DataFrame:
    path = FILES["Leverage Analysis"]
    df = pd.read_excel(path, sheet_name=SHEETS["Leverage Analysis"])
    df.columns = [clean_colname(c) for c in df.columns]

    if "years" in df.columns:
        df = df.rename(columns={"years": "year"})
    df["year"] = df["year"].astype(int)

    rename_map = {
        "repos": "repos_level",
        "repos_growth": "repos_growth",
        "deposits_level": "deposits_level",
        "deposits_growth": "deposits_growth",
        "loan_to_nominal_gdp": "loan_to_nominal_gdp",
        "credit_to_deposit_ratio": "credit_to_deposit_ratio",
    }
    df = df.rename(columns=rename_map)

    return df.sort_values("year").reset_index(drop=True)


def standardize_balance_sheet() -> pd.DataFrame:
    path = FILES["Balance Sheet Analysis"]
    df = pd.read_excel(path, sheet_name=SHEETS["Balance Sheet Analysis"])
    df.columns = [clean_colname(c) for c in df.columns]

    if "period" in df.columns:
        df["year"] = df["period"].apply(parse_year)
    else:
        df["year"] = None

    typo_fixes = {
        "capital_requiremnt": "capital_requirement",
        "capital_surplus_ration_growth": "capital_surplus_ratio_growth",
        "capital_surplus_ration": "capital_surplus_ratio",
        "leverage_ratiotrc_ta": "leverage_ratio_trc_to_ta",
    }
    df = df.rename(columns=typo_fixes)

    df = df.dropna(subset=["year"]).copy()
    df["year"] = df["year"].astype(int)

    return df.sort_values("year").reset_index(drop=True)


def write_standardized(bc: pd.DataFrame, lev: pd.DataFrame, bs: pd.DataFrame) -> None:
    bc.to_csv(PROC_DIR / "business_cycle_standardized.csv", index=False)
    lev.to_csv(PROC_DIR / "leverage_standardized.csv", index=False)
    bs.to_csv(PROC_DIR / "balance_sheet_standardized.csv", index=False)
    print("[OK] Standardized:", PROC_DIR / "business_cycle_standardized.csv", bc.shape)
    print("[OK] Standardized:", PROC_DIR / "leverage_standardized.csv", lev.shape)
    print("[OK] Standardized:", PROC_DIR / "balance_sheet_standardized.csv", bs.shape)


# -----------------------------
# Step 3: Merge master panel
# -----------------------------
def load_csv(path: Path, name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"[{name}] File not found: {path}")

    df = pd.read_csv(path)
    if "year" not in df.columns:
        raise KeyError(f"[{name}] Missing required column 'year'. Columns: {list(df.columns)}")

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"]).copy()
    df["year"] = df["year"].astype(int)

    if df["year"].duplicated().any():
        print(f"[WARN] {name}: duplicate years found; keeping first per year.")
        df = df.sort_values("year").drop_duplicates("year", keep="first")

    return df


def merge_master_panel() -> pd.DataFrame:
    bs = load_csv(PROC_DIR / "balance_sheet_standardized.csv", "balance_sheet")
    lev = load_csv(PROC_DIR / "leverage_standardized.csv", "leverage")
    bc = load_csv(PROC_DIR / "business_cycle_standardized.csv", "business_cycle")

    master = (
        bs.merge(lev, on="year", how="left", suffixes=("", "_lev"))
          .merge(bc, on="year", how="left", suffixes=("", "_bc"))
          .sort_values("year")
          .reset_index(drop=True)
    )

    master.to_csv(PROC_DIR / "master_panel.csv", index=False)
    print("[OK] Master panel:", PROC_DIR / "master_panel.csv", master.shape)
    return master


# -----------------------------
# Step 4: Consolidate (keep GDP/credit ONLY from balance sheet)
# -----------------------------
def build_consolidated_panel(master: pd.DataFrame) -> pd.DataFrame:
    df = master.copy()

    if "period" in df.columns:
        df = df.rename(columns={"period": "period_label"})

    # Drop GDP/GDP growth/Credit/Credit growth from non-balance-sheet sources
    drop_cols = [
        # GDP from other sources
        "gdp_level", "gdp_level_bc", "gdp_growth", "gdp_log", "gdp_log_growth",
        "gdp_level_consolidated",

        # Bank credit from other sources
        "bank_credit_lev", "bank_credit_growth", "bank_credit_level",
        "bank_credit_log", "bank_credit_log_growth",
        "bank_credit_level_consolidated",
        "bank_credit_log_consolidated",
        "bank_credit_log_growth_consolidated",
        "bank_credit_growth_consolidated",
    ]
    drop_cols = [c for c in drop_cols if c in df.columns]
    df = df.drop(columns=drop_cols)

    # Reorder so balance-sheet GDP/credit are first
    front = [
        "year",
        "period_label" if "period_label" in df.columns else None,
        "nominal_gdp" if "nominal_gdp" in df.columns else None,
        "log_nominal_gdp" if "log_nominal_gdp" in df.columns else None,
        "nominal_gdp_growth" if "nominal_gdp_growth" in df.columns else None,
        "bank_credit" if "bank_credit" in df.columns else None,
        "log_bank_credit" if "log_bank_credit" in df.columns else None,
        "log_bank_credit_growth" if "log_bank_credit_growth" in df.columns else None,
    ]
    front = [c for c in front if c is not None and c in df.columns]
    rest = [c for c in df.columns if c not in front]
    df = df[front + rest].sort_values("year").reset_index(drop=True)

    df.to_csv(PROC_DIR / "consolidated_panel.csv", index=False)
    print("[OK] Consolidated:", PROC_DIR / "consolidated_panel.csv", df.shape)
    return df


# -----------------------------
# Step 5: Finalize dataset (polish)
# -----------------------------
def finalize_dataset(consolidated: pd.DataFrame) -> pd.DataFrame:
    df = consolidated.copy()

    # Ensure year uniqueness
    if df["year"].duplicated().any():
        raise ValueError("Duplicate years found in consolidated panel.")

    # Normalize key numerics (safe coercion)
    key_numeric = [
        "nominal_gdp", "log_nominal_gdp", "nominal_gdp_growth",
        "bank_credit", "log_bank_credit", "log_bank_credit_growth",
    ]
    for c in key_numeric:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Keep same front ordering (already done), but re-assert for safety
    front = [
        "year",
        "period_label" if "period_label" in df.columns else None,
        "nominal_gdp",
        "log_nominal_gdp",
        "nominal_gdp_growth",
        "bank_credit",
        "log_bank_credit",
        "log_bank_credit_growth",
    ]
    front = [c for c in front if c is not None and c in df.columns]
    rest = [c for c in df.columns if c not in front]
    df = df[front + rest].sort_values("year").reset_index(drop=True)

    df.to_csv(PROC_DIR / "final_panel.csv", index=False)
    print("[OK] Final panel:", PROC_DIR / "final_panel.csv", df.shape)
    return df


# -----------------------------
# Step 6: Quality checks (write report)
# -----------------------------
def write_quality_report(df: pd.DataFrame) -> None:
    out_md = PROC_DIR / "final_panel_quality.md"

    core_cols = [
        "year",
        "nominal_gdp",
        "log_nominal_gdp",
        "nominal_gdp_growth",
        "bank_credit",
        "log_bank_credit",
        "log_bank_credit_growth",
    ]

    lines = ["# Final Panel Quality Report", ""]
    lines.append(f"- Rows: **{df.shape[0]}**")
    lines.append(f"- Columns: **{df.shape[1]}**")

    dup_years = int(df["year"].duplicated().sum())
    lines.append(f"- Duplicate years: **{dup_years}**")
    if dup_years == 0:
        lines.append(f"- Year range: **{int(df['year'].min())}–{int(df['year'].max())}**")

    lines.append("")
    lines.append("## Core column missingness")
    for c in core_cols:
        if c not in df.columns:
            lines.append(f"- ❌ `{c}`: missing column")
        else:
            miss = df[c].isna().mean()
            lines.append(f"- `{c}` missing: **{miss:.1%}**")

    lines.append("")
    lines.append("## Numeric sanity (min/max) for selected columns")
    for c in ["nominal_gdp", "bank_credit", "crar", "net_npa_ratio", "leverage_ratio_trc_to_ta", "leverage_ratiotrc_to_ta"]:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            lines.append(f"- `{c}` min/max: **{s.min()} / {s.max()}**")

    out_md.write_text("\n".join(lines), encoding="utf-8")
    print("[OK] QA report:", out_md)


# -----------------------------
# Step 7: Data dictionary template
# -----------------------------
def write_data_dictionary(df: pd.DataFrame) -> None:
    out_file = PROC_DIR / "data_dictionary.csv"

    dd = pd.DataFrame(
        {
            "variable": df.columns,
            "definition": [""] * len(df.columns),
            "source": ["RBI database on Indian Economy"] * len(df.columns),
            "unit": [""] * len(df.columns),
            "notes": [""] * len(df.columns),
        }
    )

    dd.to_csv(out_file, index=False)
    print("[OK] Data dictionary template:", out_file)


# -----------------------------
# Main runner
# -----------------------------
def main() -> int:
    print("[INFO] Repo root:", ROOT)
    print("[INFO] Data dir:", DATA_DIR)
    print("[INFO] Output dir:", OUT_DIR)
    print("[INFO] Processed dir:", PROC_DIR)

    # 1) Validate input excel files
    run_validation()

    # Hard stop if any file missing
    missing = [name for name, p in FILES.items() if not p.exists()]
    if missing:
        print(f"[ERROR] Missing input files: {missing}")
        return 1

    # 2) Extract + standardize
    bc = standardize_business_cycle()
    lev = standardize_leverage()
    bs = standardize_balance_sheet()
    write_standardized(bc, lev, bs)

    # 3) Merge master
    master = merge_master_panel()

    # 4) Consolidate (balance-sheet macro/credit only)
    consolidated = build_consolidated_panel(master)

    # 5) Finalize
    final_df = finalize_dataset(consolidated)

    # 6) QA report
    write_quality_report(final_df)

    # 7) Data dictionary template
    write_data_dictionary(final_df)

    print("\n[OK] ETL complete.")
    print("[OK] Outputs:")
    print(" -", PROC_DIR / "final_panel.csv")
    print(" -", PROC_DIR / "final_panel_quality.md")
    print(" -", PROC_DIR / "data_dictionary.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())