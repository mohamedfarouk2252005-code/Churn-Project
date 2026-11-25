# producer/fault_injector.py
# Injects one deliberately broken record per NiFi validation rule.
# Implements data_contract.md v2.3, sections 5 and 6.5.
#
# Rules:
#   Rule 1 — RULE_1_INVALID_JSON / RULE_1_MISSING_OR_TYPE
#   Rule 2 — RULE_2_BAD_ID / RULE_2_BAD_DATE
#   Rule 3 — RULE_3_OUT_OF_RANGE
#   Rule 4 — RULE_4_INVALID_ENUM
#   Rule 5 — RULE_5_CROSS_FIELD
#   Rule 6 — RULE_6_CHURN_INCONSISTENT
#
# IMPORTANT: each error must be un-fixable by NiFi cleaning (§6.5).
# Wrong casing or extra spaces are NOT useful because NiFi cleans them.
#
# Agree the exact error_reason codes with Boda before finalizing this file.

# A clean base record to copy and corrupt (matches the sample in data_contract.md §10.1).
_BASE = {
    "customer_id": "C_FAULT_BASE",
    "gender": "Male",
    "age": 35,
    "under_30": "No",
    "senior_citizen": "No",
    "married": "No",
    "dependents": "No",
    "number_of_dependents": 0,
    "city": "Sacramento",
    "referred_a_friend": "No",
    "number_of_referrals": 0,
    "tenure_in_months": 12,
    "offer": None,
    "phone_service": "Yes",
    "avg_monthly_long_distance_charges": 20.00,
    "multiple_lines": "No",
    "internet_service": "Yes",
    "internet_type": "DSL",
    "avg_monthly_gb_download": 10,
    "premium_tech_support": "No",
    "streaming_tv": "No",
    "streaming_movies": "No",
    "streaming_music": "No",
    "unlimited_data": "No",
    "contract": "Month-to-Month",
    "paperless_billing": "Yes",
    "payment_method": "Bank Withdrawal",
    "monthly_charge": 60.00,
    "total_charges": 720.00,
    "total_refunds": 0.0,
    "total_extra_data_charges": 0,
    "total_long_distance_charges": 240.00,
    "total_revenue": 960.00,
    "satisfaction_score": 3,
    "customer_status": "Stayed",
    "churn_label": "No",
    "churn_score": 40,
    "cltv": 3000,
    "churn_category": None,
    "churn_reason": None,
    "customer_service_calls_1m": 1,
    "customer_service_calls_3m": 2,
    "income": 5000,
    "churn_date": None,
    "calls_1m": 50,
    "calls_3m": 150,
    "sms_1m": 30,
    "sms_3m": 90,
    "purchases_1m": 1,
    "purchases_3m": 3,
    "data_usage_gb_1m": 10.0,
    "data_usage_gb_3m": 30,
    "event_timestamp": "2026-11-25T00:00:00Z",
}


def _fault(customer_id: str, **overrides) -> dict:
    """Return a copy of _BASE with the given fields replaced."""
    record = dict(_BASE)
    record["customer_id"] = customer_id
    record.update(overrides)
    return record


# ── One fault record per rule ─────────────────────────────────────────────────

FAULT_RECORDS = [

    # ── Rule 1a: missing required field (city is absent entirely)
    # NiFi Rule 1: "The JSON message contains all 53 contracted fields."
    {k: v for k, v in _fault("C_FAULT_R1A").items() if k != "city"},

    # ── Rule 1b: wrong JSON type — age sent as a string instead of int
    # NiFi Rule 1: "Numeric fields are JSON numbers, not strings."
    _fault("C_FAULT_R1B", age="thirty-five"),

    # ── Rule 2a: customer_id does not match ^C\d{9}$
    # NiFi Rule 2: "customer_id is non-empty and matches ^C\d{9}$."
    _fault("X_INVALID_ID"),

    # ── Rule 2b: churn_date outside Q3 2025 (after cleaning → 2025-11-07)
    # The date "11/7/2025" → month-first → 2025-11-07, outside 2025-07-01 to 2025-09-30.
    # NiFi Rule 2: "churn_date is a valid YYYY-MM-DD date between 2025-07-01 and 2025-09-30."
    _fault("C_FAULT_R2B",
           customer_status="Churned",
           churn_label="Yes",
           churn_category="Price",
           churn_reason="Price too high",
           churn_date="11/7/2025"),

    # ── Rule 3: satisfaction_score out of range (must be 1–5)
    # NiFi Rule 3: "satisfaction_score is between 1 and 5."
    _fault("C_FAULT_R3A", satisfaction_score=7),

    # ── Rule 4: payment_method not in the allowed list
    # NiFi Rule 4: "payment_method belongs strictly to its allowed list."
    # Allowed: "Bank Withdrawal", "Credit Card", "Mailed Check"
    _fault("C_FAULT_R4A", payment_method="Cash"),

    # ── Rule 5: cross-field inconsistency — internet_service "No" but internet_type set
    # NiFi Rule 5: "If internet_service = 'No': internet_type is null."
    _fault("C_FAULT_R5A",
           internet_service="No",
           internet_type="Fiber Optic",   # must be null when no internet
           avg_monthly_gb_download=0,
           data_usage_gb_1m=0.0,
           data_usage_gb_3m=0,
           total_extra_data_charges=0,
           premium_tech_support="No",
           streaming_tv="No",
           streaming_movies="No",
           streaming_music="No",
           unlimited_data="No"),

    # ── Rule 6: churn_label "Yes" but customer_status "Stayed"
    # NiFi Rule 6: "customer_status = 'Churned' if and only if churn_label = 'Yes'."
    _fault("C_FAULT_R6A",
           churn_label="Yes",
           customer_status="Stayed",   # contradicts churn_label
           churn_category=None,
           churn_reason=None,
           churn_date=None),
]


def get_fault_records() -> list:
    """Return the list of fault records to interleave into the stream."""
    return FAULT_RECORDS
