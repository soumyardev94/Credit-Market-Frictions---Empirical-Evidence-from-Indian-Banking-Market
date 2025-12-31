from pathlib import Path
from typing import Literal
import pandas as pd
import statsmodels.api as sm

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "outputs" / "processed" / "final_panel.csv"
OUT_TBL = ROOT / "reports" / "tables"
OUT_TBL.mkdir(parents=True, exist_ok=True)


Y_COL = "log_bank_credit_growth"

# Candidate regressors (only keep those that exist in your final_panel.csv)
CANDIDATES = [
    "capital_surplus_ratio",
    "crar",
    "nominal_gdp_growth",
    "net_npa_ratio",
    "change_in_rw",
    "leverage_ratiotrc_to_ta",
]

# Define multiple specs to test robustness
SPECS = {
    "spec_1_baseline": [
        "capital_surplus_ratio",
        "nominal_gdp_growth",
        "net_npa_ratio",
        "change_in_rw",
        "leverage_ratiotrc_to_ta",
    ],
    "spec_2_replace_capital_with_crar": [
        "crar",
        "nominal_gdp_growth",
        "net_npa_ratio",
        "change_in_rw",
        "leverage_ratiotrc_to_ta",
    ],
    "spec_3_core_risk_only": [
        "net_npa_ratio",
        "change_in_rw",
        "nominal_gdp_growth",
    ],
    "spec_4_drop_gdp_control": [
        "capital_surplus_ratio",
        "net_npa_ratio",
        "change_in_rw",
        "leverage_ratiotrc_to_ta",
    ],
}
CovType = Literal["HC0", "HC1", "HC2", "HC3", "nonrobust", "HAC", "cluster", "fixed scale", "hac-panel", "hac-groupsum"]
def fit_ols(df: pd.DataFrame, y_col: str, x_cols: list[str], cov_type: CovType = "HC1"):
    y = pd.to_numeric(df[y_col], errors="coerce")
    X = df[x_cols].apply(pd.to_numeric, errors="coerce")

    data = pd.concat([y, X], axis=1).dropna()
    y2 = data[y_col]
    X2 = sm.add_constant(data.drop(columns=[y_col]))

    model = sm.OLS(y2, X2).fit(cov_type=cov_type)
    return model


def tidy_params(model) -> pd.DataFrame:
    """Return a tidy table with coef, robust se, p, CI for each parameter."""
    params = model.params
    bse = model.bse
    pvals = model.pvalues
    conf = model.conf_int()
    out = pd.DataFrame({
        "coef": params,
        "robust_se": bse,
        "p_value": pvals,
        "ci_low": conf[0],
        "ci_high": conf[1],
    })
    out.index.name = "term"
    return out


def main():
    if not DATA.exists():
        raise FileNotFoundError(f"Missing: {DATA}")

    df = pd.read_csv(DATA)

    if Y_COL not in df.columns:
        raise KeyError(f"Missing dependent variable '{Y_COL}' in {DATA}")

    # Keep only regressors that actually exist in the dataset
    available = set(df.columns)
    specs_filtered = {}
    for spec_name, cols in SPECS.items():
        cols_ok = [c for c in cols if c in available]
        if len(cols_ok) == 0:
            print(f"[WARN] {spec_name}: no regressors found in dataset. Skipping.")
            continue
        specs_filtered[spec_name] = cols_ok

    results_meta = []
    comparison_blocks = []

    for spec_name, x_cols in specs_filtered.items():
        model = fit_ols(df, Y_COL, x_cols, cov_type="HC1")

        # Save full tidy regression table
        tidy = tidy_params(model)
        out_file = OUT_TBL / f"{spec_name}.csv"
        tidy.to_csv(out_file)
        print("[OK] Wrote:", out_file)

        # Store model summary stats
        results_meta.append({
            "spec": spec_name,
            "n_obs": int(model.nobs),
            "r2": float(model.rsquared),
            "adj_r2": float(model.rsquared_adj),
            "aic": float(model.aic),
            "bic": float(model.bic),
        })

        # Build a comparison block: coef (se) with p
        block = tidy[["coef", "robust_se", "p_value"]].copy()
        block.columns = pd.MultiIndex.from_product([[spec_name], block.columns])
        comparison_blocks.append(block)

    # Write model comparison metrics
    meta_df = pd.DataFrame(results_meta).sort_values("spec")
    meta_df.to_csv(OUT_TBL / "robustness_model_metrics.csv", index=False)
    print("[OK] Wrote:", OUT_TBL / "robustness_model_metrics.csv")

    # Write a combined comparison table
    if comparison_blocks:
        comp = pd.concat(comparison_blocks, axis=1)
        comp.to_csv(OUT_TBL / "robustness_comparison_table.csv")
        print("[OK] Wrote:", OUT_TBL / "robustness_comparison_table.csv")

    print("\n[OK] Robustness run complete.")


if __name__ == "__main__":
    main()