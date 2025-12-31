# src/analysis/03_make_figures.py
# Creates clean portfolio figures from final_panel.csv
# Outputs:
#   reports/figures/01_credit_log_growth.png
#   reports/figures/02_capital_surplus_ratio.png
#   reports/figures/03_scatter_capital_surplus_vs_credit_growth.png
#   reports/figures/04_npa_ratio.png (if available)

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "outputs" / "processed" / "final_panel.csv"
FIG_DIR = ROOT / "reports" / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

def _num(df: pd.DataFrame, col: str):
    return pd.to_numeric(df[col], errors="coerce")

def save_line(df: pd.DataFrame, x: str, y: str, title: str, filename: str, ylabel: str | None = None):
    plt.figure()
    plt.plot(df[x], df[y], marker="o")
    plt.title(title)
    plt.xlabel(x)
    plt.ylabel(ylabel if ylabel else y)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    out = FIG_DIR / filename
    plt.savefig(out, dpi=200)
    plt.close()
    print("[OK] Wrote:", out)

def save_scatter(df: pd.DataFrame, x: str, y: str, title: str, filename: str, xlabel: str | None = None, ylabel: str | None = None):
    plt.figure()
    plt.scatter(df[x], df[y])
    plt.title(title)
    plt.xlabel(xlabel if xlabel else x)
    plt.ylabel(ylabel if ylabel else y)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    out = FIG_DIR / filename
    plt.savefig(out, dpi=200)
    plt.close()
    print("[OK] Wrote:", out)

def main():
    if not DATA.exists():
        raise FileNotFoundError(f"Missing: {DATA}")

    df = pd.read_csv(DATA)

    # Ensure 'year' numeric
    if "year" not in df.columns:
        raise KeyError("final_panel.csv must contain 'year'")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"]).copy()
    df["year"] = df["year"].astype(int)
    df = df.sort_values("year").reset_index(drop=True)

    # --- Figure 1: Log credit growth over time ---
    if "log_bank_credit_growth" in df.columns:
        d1 = df[["year", "log_bank_credit_growth"]].copy()
        d1["log_bank_credit_growth"] = _num(d1, "log_bank_credit_growth")
        d1 = d1.dropna()
        save_line(
            d1,
            x="year",
            y="log_bank_credit_growth",
            title="Bank Credit Growth (log) over time",
            filename="01_credit_log_growth.png",
            ylabel="log growth"
        )
    else:
        print("[WARN] Missing column: log_bank_credit_growth (skipping Figure 1)")

    # --- Figure 2: Capital surplus ratio over time ---
    if "capital_surplus_ratio" in df.columns:
        d2 = df[["year", "capital_surplus_ratio"]].copy()
        d2["capital_surplus_ratio"] = _num(d2, "capital_surplus_ratio")
        d2 = d2.dropna()
        save_line(
            d2,
            x="year",
            y="capital_surplus_ratio",
            title="Capital Surplus Ratio over time",
            filename="02_capital_surplus_ratio.png",
            ylabel="ratio"
        )
    else:
        print("[WARN] Missing column: capital_surplus_ratio (skipping Figure 2)")

    # --- Figure 3: Scatter - capital surplus ratio vs log credit growth ---
    if "capital_surplus_ratio" in df.columns and "log_bank_credit_growth" in df.columns:
        d3 = df[["capital_surplus_ratio", "log_bank_credit_growth"]].copy()
        d3["capital_surplus_ratio"] = _num(d3, "capital_surplus_ratio")
        d3["log_bank_credit_growth"] = _num(d3, "log_bank_credit_growth")
        d3 = d3.dropna()
        save_scatter(
            d3,
            x="capital_surplus_ratio",
            y="log_bank_credit_growth",
            title="Capital Surplus Ratio vs Bank Credit Growth (log)",
            filename="03_scatter_capital_surplus_vs_credit_growth.png",
            xlabel="capital surplus ratio",
            ylabel="log credit growth"
        )
    else:
        print("[WARN] Missing columns for Figure 3 (skipping)")

    # --- Figure 4: NPA ratio over time (optional) ---
    if "net_npa_ratio" in df.columns:
        d4 = df[["year", "net_npa_ratio"]].copy()
        d4["net_npa_ratio"] = _num(d4, "net_npa_ratio")
        d4 = d4.dropna()
        save_line(
            d4,
            x="year",
            y="net_npa_ratio",
            title="Net NPA Ratio over time",
            filename="04_npa_ratio.png",
            ylabel="ratio"
        )
    else:
        print("[WARN] Missing column: net_npa_ratio (skipping Figure 4)")

    print("\n[OK] Figure generation complete.")
    print("[OK] Figures in:", FIG_DIR)


if __name__ == "__main__":
    main()