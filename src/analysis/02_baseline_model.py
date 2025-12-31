from pathlib import Path
import pandas as pd
import statsmodels.api as sm

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "outputs" / "processed" / "final_panel.csv"
OUT_TBL = ROOT / "reports" / "tables"
OUT_TBL.mkdir(parents=True, exist_ok=True)

def main():
    df = pd.read_csv(DATA)

    y = df["log_bank_credit_growth"]
    X = df[[
        "capital_surplus_ratio",
        "nominal_gdp_growth",
        "net_npa_ratio",
        "change_in_rw",
        "leverage_ratiotrc_to_ta",
    ]].copy()

    # Ensure numeric + drop missing rows
    X = X.apply(pd.to_numeric, errors="coerce")
    y = pd.to_numeric(y, errors="coerce")

    data = pd.concat([y, X], axis=1).dropna()
    y2 = data["log_bank_credit_growth"]
    X2 = sm.add_constant(data.drop(columns=["log_bank_credit_growth"]))

    model = sm.OLS(y2, X2).fit(cov_type="HC1")  # robust SE
    print(model.summary())

    # Save a clean table
    out = model.summary2().tables[1]
    out.to_csv(OUT_TBL / "baseline_regression.csv")
    print("[OK] Wrote:", OUT_TBL / "baseline_regression.csv")

if __name__ == "__main__":
    main()