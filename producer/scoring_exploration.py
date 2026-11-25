# scoring_exploration.py
# ── Killy's pandas analysis script ──────────────────────────────────────────
# Run this on the raw CSV to discover which fields best predict churn.
# Share the output with Mohamed to help him finalize docs/scoring_rules.md.
#
# Usage (from the project root):
#   pip install pandas
#   python producer/scoring_exploration.py

import pandas as pd
import sys
import os

DATASET = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "churn_raw.csv")


def load() -> pd.DataFrame:
    print(f"Loading dataset from: {DATASET}\n")
    df = pd.read_csv(DATASET)
    print(f"Rows: {len(df):,}   Columns: {len(df.columns)}\n")
    return df


def section(title: str) -> None:
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def run(df: pd.DataFrame) -> None:

    section("1. Overall churn rate")
    rates = df["churn_label"].value_counts(normalize=True).mul(100).round(1)
    print(rates.to_string())

    section("2. Churn rate by CONTRACT type")
    tbl = (
        df.groupby("contract")["churn_label"]
        .value_counts(normalize=True)
        .mul(100).round(1)
        .unstack(fill_value=0)
    )
    print(tbl.to_string())

    section("3. Churn rate by INTERNET TYPE")
    tbl = (
        df.groupby("internet_type")["churn_label"]
        .value_counts(normalize=True)
        .mul(100).round(1)
        .unstack(fill_value=0)
    )
    print(tbl.to_string())

    section("4. Avg CUSTOMER SERVICE CALLS — churned vs stayed")
    cols = ["customer_service_calls_1m", "customer_service_calls_3m"]
    print(df.groupby("churn_label")[cols].mean().round(2).to_string())

    section("5. Churn rate by SATISFACTION SCORE (1–5)")
    tbl = (
        df.groupby("satisfaction_score")["churn_label"]
        .value_counts(normalize=True)
        .mul(100).round(1)
        .unstack(fill_value=0)
    )
    print(tbl.to_string())

    section("6. Churn rate by TENURE bucket (months)")
    df["tenure_bucket"] = pd.cut(
        df["tenure_in_months"],
        bins=[0, 6, 12, 24, 48, 999],
        labels=["0-6m", "7-12m", "13-24m", "25-48m", "48m+"],
    )
    tbl = (
        df.groupby("tenure_bucket", observed=True)["churn_label"]
        .value_counts(normalize=True)
        .mul(100).round(1)
        .unstack(fill_value=0)
    )
    print(tbl.to_string())

    section("7. Avg MONTHLY CHARGE — churned vs stayed")
    print(df.groupby("churn_label")["monthly_charge"].describe().round(2).to_string())

    section("8. Top 10 CHURN REASONS")
    churned = df[df["churn_label"] == "Yes"]
    print(churned["churn_reason"].value_counts().head(10).to_string())

    section("9. Churn rate by PAYMENT METHOD")
    tbl = (
        df.groupby("payment_method")["churn_label"]
        .value_counts(normalize=True)
        .mul(100).round(1)
        .unstack(fill_value=0)
    )
    print(tbl.to_string())

    section("10. DATA USAGE (GB/month) — churned vs stayed")
    cols = ["data_usage_gb_1m", "data_usage_gb_3m"]
    print(df.groupby("churn_label")[cols].mean().round(2).to_string())

    section("11. Churn rate — SENIOR CITIZEN vs not")
    tbl = (
        df.groupby("senior_citizen")["churn_label"]
        .value_counts(normalize=True)
        .mul(100).round(1)
        .unstack(fill_value=0)
    )
    print(tbl.to_string())

    section("12. Correlation of numeric fields with churn (1=churned)")
    df["churn_num"] = (df["churn_label"] == "Yes").astype(int)
    numeric_cols = [
        "age", "tenure_in_months", "monthly_charge", "satisfaction_score",
        "customer_service_calls_1m", "customer_service_calls_3m",
        "data_usage_gb_1m", "calls_1m", "sms_1m", "cltv", "churn_score",
    ]
    existing = [c for c in numeric_cols if c in df.columns]
    corr = df[existing + ["churn_num"]].corr()["churn_num"].drop("churn_num").sort_values()
    print(corr.round(3).to_string())
    print("\n(negative = more of this → less likely to churn)")
    print("(positive = more of this → more likely to churn)")


if __name__ == "__main__":
    if not os.path.exists(DATASET):
        print(f"ERROR: Dataset not found at {DATASET}")
        print("Place churn_raw.csv in data/raw/ first.")
        sys.exit(1)
    df = load()
    run(df)
    print("\n\nDone. Share this output with Mohamed for scoring_rules.md.")
