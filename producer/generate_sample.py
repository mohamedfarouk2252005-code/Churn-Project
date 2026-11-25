# producer/generate_sample.py
# One-off script — generates producer/data/sample_valid_records.json
# Picks 100 records that pass all 6 NiFi validation rules.
# Run once from the project root or producer/ folder:
#   python producer/generate_sample.py

import csv
import json
import os
import re
import sys

DATASET   = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "churn_raw.csv")
OUT_FILE  = os.path.join(os.path.dirname(__file__), "data", "sample_valid_records.json")
TARGET    = 100

EXCLUDED  = {"country", "state", "quarter", "zip_code", "population", "churn_value"}
NULLABLE  = {"age", "offer", "internet_type", "total_charges",
             "churn_date", "churn_category", "churn_reason"}
INT_FIELDS = {
    "age", "number_of_dependents", "number_of_referrals", "tenure_in_months",
    "avg_monthly_gb_download", "total_extra_data_charges", "satisfaction_score",
    "churn_score", "cltv", "customer_service_calls_1m", "customer_service_calls_3m",
    "income", "calls_1m", "calls_3m", "sms_1m", "sms_3m",
    "purchases_1m", "purchases_3m", "data_usage_gb_3m",
}
FLOAT_FIELDS = {
    "avg_monthly_long_distance_charges", "monthly_charge",
    "total_charges", "total_refunds", "total_long_distance_charges",
    "total_revenue", "data_usage_gb_1m",
}

ID_RE = re.compile(r"^C\d{9}$")

Q3_START = "2025-07-01"
Q3_END   = "2025-09-30"


def is_valid(row: dict) -> bool:
    """Return True if the row would pass all 6 NiFi validation rules."""

    # Rule 2 — customer_id
    cid = row.get("customer_id", "")
    if not ID_RE.match(cid):
        return False

    # Rule 2 — churn_date in Q3 if present
    cd = row.get("churn_date", "")
    if cd:
        if not (Q3_START <= cd <= Q3_END):
            return False

    # Rule 3 — ranges
    age = row.get("age", "")
    if age not in ("", None):
        try:
            a = int(float(age))
            if not (18 <= a <= 100):
                return False
        except (ValueError, TypeError):
            return False

    tenure = row.get("tenure_in_months", "")
    try:
        t = int(float(tenure))
        if not (0 <= t <= 72):
            return False
    except (ValueError, TypeError):
        return False

    mc = row.get("monthly_charge", "")
    try:
        m = float(mc)
        if not (0 < m <= 200):
            return False
    except (ValueError, TypeError):
        return False

    ss = row.get("satisfaction_score", "")
    try:
        s = int(float(ss))
        if not (1 <= s <= 5):
            return False
    except (ValueError, TypeError):
        return False

    cs = row.get("churn_score", "")
    try:
        c = int(float(cs))
        if not (0 <= c <= 100):
            return False
    except (ValueError, TypeError):
        return False

    return True


def serialize(name: str, raw: str):
    if raw == "" and name in NULLABLE:
        return None
    if name in INT_FIELDS:
        try:
            f = float(raw)
            if f == int(f):
                return int(f)
        except (ValueError, TypeError):
            pass
        return raw
    if name in FLOAT_FIELDS:
        try:
            return float(raw)
        except (ValueError, TypeError):
            return raw
    return raw


def build_message(row: dict) -> dict:
    msg = {}
    for k, v in row.items():
        if k in EXCLUDED:
            continue
        msg[k] = serialize(k, v)
    # event_timestamp placeholder for sample data
    msg["event_timestamp"] = "2026-11-25T00:00:00Z"
    return msg


def main():
    if not os.path.exists(DATASET):
        print(f"ERROR: Dataset not found at {DATASET}")
        sys.exit(1)

    samples = []
    print(f"Scanning {DATASET} for valid records ...")

    with open(DATASET, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if is_valid(row):
                samples.append(build_message(row))
            if len(samples) >= TARGET:
                break

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(samples, f, indent=2, default=str)

    print(f"Done — {len(samples)} valid records written to {OUT_FILE}")


if __name__ == "__main__":
    main()
