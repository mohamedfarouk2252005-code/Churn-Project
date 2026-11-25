# producer/dataset_loader.py
# Reads churn_raw.csv and yields rows as raw dicts (string values only).
# Implements data_contract.md v2.3, section 6.3:
#   "send rows in FILE ORDER" (not sorted by churn_date).
#
# The producer does NO type conversion or cleaning here.
# serialize_field() in producer.py handles all type casting.

import csv
import logging
import os
import sys

import config

log = logging.getLogger(__name__)

# All 52 columns the producer MUST send (data_contract.md §2 + event_timestamp added by producer).
# The 6 excluded columns are listed in config.EXCLUDED_COLUMNS.
REQUIRED_COLUMNS = {
    "customer_id", "gender", "age", "under_30", "senior_citizen", "married",
    "dependents", "number_of_dependents", "city", "referred_a_friend",
    "number_of_referrals", "tenure_in_months", "offer", "phone_service",
    "avg_monthly_long_distance_charges", "multiple_lines", "internet_service",
    "internet_type", "avg_monthly_gb_download", "premium_tech_support",
    "streaming_tv", "streaming_movies", "streaming_music", "unlimited_data",
    "contract", "paperless_billing", "payment_method", "monthly_charge",
    "total_charges", "total_refunds", "total_extra_data_charges",
    "total_long_distance_charges", "total_revenue", "satisfaction_score",
    "customer_status", "churn_label", "churn_score", "cltv",
    "churn_category", "churn_reason", "customer_service_calls_1m",
    "customer_service_calls_3m", "income", "churn_date",
    "calls_1m", "calls_3m", "sms_1m", "sms_3m",
    "purchases_1m", "purchases_3m", "data_usage_gb_1m", "data_usage_gb_3m",
}


def _resolve_path(path: str) -> str:
    """Resolve dataset path relative to the project root (one level up from producer/)."""
    if os.path.isabs(path):
        return path
    # When running from project root: data/raw/churn_raw.csv
    # When running from producer/: ../data/raw/churn_raw.csv
    candidates = [
        path,
        os.path.join(os.path.dirname(__file__), "..", path),
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return os.path.normpath(candidate)
    return path   # let the caller handle the missing file


def load_records():
    """
    Read churn_raw.csv row by row and yield each row as a dict of raw strings.

    Contract rules enforced here:
    - §6.1  Drops the 6 excluded columns.
    - §6.1  Stops with an error if a required column is missing or a header is empty.
    - §6.3  Rows are yielded in FILE ORDER (no sorting).
    - §6.2  No type conversion, cleaning or repair — that is producer.py's job.
    """
    path = _resolve_path(config.DATASET_PATH)

    if not os.path.exists(path):
        log.error("Dataset not found: %s", path)
        log.error("Place churn_raw.csv in data/raw/ (see data/README.md).")
        sys.exit(1)

    log.info("Opening dataset: %s", path)

    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)

        # §6.1: fail fast if any header is empty
        for col in (reader.fieldnames or []):
            if col.strip() == "":
                log.error("Dataset has an empty column header — aborting.")
                sys.exit(1)

        # §6.1: fail fast if a required column is missing
        file_columns = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - file_columns - config.EXCLUDED_COLUMNS
        if missing:
            log.error("Dataset is missing required columns: %s", sorted(missing))
            sys.exit(1)

        for row in reader:
            # Drop the 6 excluded columns (§6.1)
            yield {k: v for k, v in row.items() if k not in config.EXCLUDED_COLUMNS}
