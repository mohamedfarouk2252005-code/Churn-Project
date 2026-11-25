# producer/config.py
# Central configuration — change values here, nowhere else.
# Source of truth: docs/data_contract.md v2.3

# ── Kafka ────────────────────────────────────────────────────────────────────
# From your laptop the producer connects to the EXTERNAL listener on port 9094.
# Containers talk to each other on kafka:9092 (internal).
KAFKA_BOOTSTRAP = "localhost:9094"   # EXTERNAL listener — use this from your laptop
TOPIC_RAW       = "churn.raw"        # topic the producer publishes to

# ── Dataset ──────────────────────────────────────────────────────────────────
DATASET_PATH    = "data/raw/churn_raw.csv"   # relative to project root

# ── Producer behaviour ───────────────────────────────────────────────────────
DEFAULT_RATE    = 10     # records per second when --rate is not given

# ── Fields NOT sent to Kafka (data_contract.md §6.1) ─────────────────────────
# country, state, quarter → constant value across all rows
# zip_code, population   → not used by scoring or dashboard
# churn_value            → duplicate of churn_label as 0/1
EXCLUDED_COLUMNS = {"country", "state", "quarter", "zip_code", "population", "churn_value"}

# ── Nullable fields (data_contract.md §4) ────────────────────────────────────
# Empty CSV cell → null JSON for these fields only. All others stay as empty string.
NULLABLE_FIELDS = {"age", "offer", "internet_type", "total_charges",
                   "churn_date", "churn_category", "churn_reason"}

# ── Integer fields (data_contract.md §2) ─────────────────────────────────────
# These must be emitted as JSON integers (whole numbers), not strings or floats.
INTEGER_FIELDS = {
    "age", "number_of_dependents", "number_of_referrals", "tenure_in_months",
    "avg_monthly_gb_download", "total_extra_data_charges", "satisfaction_score",
    "churn_score", "cltv", "customer_service_calls_1m", "customer_service_calls_3m",
    "income", "calls_1m", "calls_3m", "sms_1m", "sms_3m",
    "purchases_1m", "purchases_3m", "data_usage_gb_3m",
}

# ── Float/number fields (data_contract.md §2) ────────────────────────────────
# These must be emitted as JSON numbers (not strings).
FLOAT_FIELDS = {
    "avg_monthly_long_distance_charges", "monthly_charge",
    "total_charges", "total_refunds", "total_long_distance_charges",
    "total_revenue", "data_usage_gb_1m",
}
