# 📜 Data Contract: Customer Snapshot (v1.3)

**Owner:** Member 1 (Leader)  |  **Dictionary maintained by:** Member 6  |  **Topics:** `churn.raw`, `churn.clean`, `churn.dlq` (Key = `customer_id`)

Each Kafka message is a **snapshot of one customer at a point in time**, serialized in UTF-8 JSON. This file is the **single source of truth** for field names, types and rules. Any change must be approved by Member 1 and announced to the entire team the same day.

> **Flow note (2026-11-18):** the source of the messages is now a dataset replayed by a Python producer, and the flagging of customers at risk is done in Spark with points (`docs/scoring_rules.md`, to be agreed). **The field definitions, rules and samples in this contract are unchanged.** They will be revisited once the team chooses the dataset.

---

## 1. Time Windows and Time Semantics

All counts are relative to `event_timestamp`.

| Field Group | Window |
|---|---|
| `calls_count`, `messages_count`, `data_usage_mb` | Last 30 days |
| `purchase_frequency` | Last 30 days |
| `cs_calls_last_7d` | Last 7 days |
| `cs_calls_last_30d` | Last 30 days |
| `plan_changes_6m` and all `last_plan_change*` fields | Last 6 months (180 days) |

**Time semantics:**
- `event_timestamp` is the **real wall-clock time (UTC)** at the moment the generator creates the snapshot. It is **not** a simulated clock, so Spark windows and watermarks work on live data.
- Plan-change history is stored with real dates. When the generator creates a customer, it **backdates** that customer's plan changes to random dates in the past (up to 365 days), so some changes are already outside the 180-day window from the first snapshot.

---

## 2. Raw Fields (`churn.raw`)

| Field | Type | Allowed Values / Rule | Meaning |
|---|---|---|---|
| `customer_id` | string | `C` + digits, e.g. `C10023` | Unique customer ID (also the Kafka message key) |
| `age` | int | Generator range: `18` to `100`. Validation range: `0` to `120` | Customer age in years |
| `gender` | string | `M`, `F` | Customer gender |
| `location` | string | `Cairo`, `Alexandria`, `Giza`, `Mansoura`, `Tanta`, `Aswan`, `Suez`, `Assiut` | Customer city |
| `income_level` | string | `low`, `medium`, `high` | Customer income bracket |
| `tenure_months` | int | `>= 0` | Months since the customer joined |
| `calls_count` | int | `>= 0` | Voice calls made in the last 30 days |
| `messages_count` | int | `>= 0` | SMS messages sent in the last 30 days |
| `data_usage_mb` | number | `>= 0.0` | Internet data used in the last 30 days (MB) |
| `plan_id` | int | `1` to `5` | Current subscription plan ID (see Section 3) |
| `plan_speed_mbps` | int | Must match `plan_id` | Speed of the current plan (Mbps) |
| `monthly_charges` | number | Must match `plan_id` | Monthly subscription price in EGP |
| `payment_method` | string | `credit_card`, `debit_card`, `cash`, `mobile_wallet` | Billing payment method |
| `last_plan_changed_from` | int or null | `1` to `5`, or `null` | Plan ID before the most recent change **within 6 months** |
| `last_plan_changed_to` | int or null | `1` to `5`, or `null` | Plan ID after the most recent change **within 6 months** |
| `last_plan_change_type` | string | `upgrade`, `downgrade`, `none` | Type of the most recent plan change within 6 months |
| `plan_changes_6m` | int | `>= 0` | Number of plan changes in the last 6 months (180 days) |
| `cs_calls_last_7d` | int | `>= 0` | Customer service calls in the last 7 days |
| `cs_calls_last_30d` | int | `>= 0` | Customer service calls in the last 30 days |
| `last_complaint_type` | string | `slow_internet`, `frequent_disconnection`, `billing_issue`, `technical_fault`, `plan_inquiry`, `other`, `none` | Category of the most recent complaint |
| `purchase_frequency` | int | `>= 0` | Add-on or top-up purchases in the last 30 days |
| `days_since_last_purchase` | int or null | `>= 0`; `null` = never purchased | Days since the last purchase |
| `churn_status` | string | `Yes`, `No` | Whether the customer has cancelled |
| `churn_date` | string or null | `YYYY-MM-DD`; `null` if `churn_status = "No"` | Date the customer churned |
| `event_timestamp` | string | ISO 8601 UTC (`YYYY-MM-DDTHH:mm:ssZ`) | When the snapshot was generated |

---

## 3. Plan Catalog

| `plan_id` | Name | `plan_speed_mbps` | `monthly_charges` (EGP) |
|---|---|---|---|
| 1 | Starter | 30 | 200 |
| 2 | Basic | 50 | 300 |
| 3 | Plus | 70 | 400 |
| 4 | Pro | 100 | 550 |
| 5 | Ultra | 150 | 750 |

- **Upgrade:** `last_plan_changed_to > last_plan_changed_from`
- **Downgrade:** `last_plan_changed_to < last_plan_changed_from`
- **Master file:** `reference/plan_catalog.csv`. The copies in `generator/config.py`, `nifi/lookup_tables/plan_catalog.csv` and `sql/04_seed_plans.sql` must match it. `scripts/verify_pipeline.py` checks this.

---

## 4. Processing Order and Cleaning Limits (NiFi)

**Order:** Consume `churn.raw` → **Clean** → **Validate** (Rules 1 to 8) → **Enrich** → Publish to `churn.clean` (valid) or `churn.dlq` (invalid).

Cleaning only fixes **formatting**. It is allowed to:
- Trim leading and trailing spaces in strings.
- Fix the letter case of enum values (`cairo` → `Cairo`, `yes` → `Yes`, `CASH` → `cash`).
- Map gender words: `male` / `Male` → `M`, `female` / `Female` → `F`.
- Convert a date or timestamp to the required format (for example `2026-11-14 10:30:00` → `2026-11-14T10:30:00Z`).
- Convert an empty string `""` to `null` in nullable fields.

Cleaning must **never**:
- Change a number or a data type (a number sent as a string fails Rule 1).
- Fix a plan/price mismatch.
- Fill in a missing required field.
- Correct a value that is not in its allowed list (gender `"X"` fails Rule 8).

This keeps errors visible: anything cleaning cannot repair must reach `churn.dlq`.

---

## 5. Validation Rules (Enforced by Member 2 in NiFi)

A record must pass **all 8 rules** to be routed to `churn.clean`. Rules are evaluated **in order 1 to 8**, and the code of the **first failed rule** is stored in `error_reason` (see Section 8).

1. **Parsing, presence and types:**
   - The message is valid JSON.
   - All required fields are present (non-null, except the nullable fields marked above).
   - Numeric fields are JSON numbers, not strings. Integer fields are whole numbers.
   - All counts are `>= 0`, and `age` is between `0` and `120`.
2. **Plan and price consistency:** `plan_speed_mbps` and `monthly_charges` match `plan_id` according to Section 3. Numbers are compared **numerically**, so `400` and `400.0` are both valid for plan 3.
3. **CS call window consistency:** `cs_calls_last_7d <= cs_calls_last_30d`.
4. **Zero-call complaint consistency:** if `cs_calls_last_30d == 0`, then `last_complaint_type` is `"none"`.
5. **Plan changes consistency (6-month window):**
   - If `plan_changes_6m == 0`: `last_plan_changed_from` and `last_plan_changed_to` are `null`, and `last_plan_change_type` is `"none"`.
   - If `plan_changes_6m >= 1`: both plan fields are non-null, `last_plan_changed_from != last_plan_changed_to`, `last_plan_changed_to == plan_id`, and `last_plan_change_type` matches the direction (`to > from` → `"upgrade"`, `to < from` → `"downgrade"`).
6. **Purchases consistency:**
   - If `days_since_last_purchase` is `null`, then `purchase_frequency == 0`.
   - If `purchase_frequency >= 1`, then `days_since_last_purchase` is non-null and `<= 30`.
   - If `purchase_frequency == 0`, then `days_since_last_purchase` is `null` or `> 30`.
7. **Churn status and date consistency:**
   - `churn_date` is `null` if and only if `churn_status == "No"`.
   - If `churn_status == "Yes"`, `churn_date` is a valid `YYYY-MM-DD` date that is **not later than the date of `event_timestamp`**.
8. **Allowed enum values:** `gender`, `location`, `income_level`, `payment_method`, `last_plan_change_type`, `last_complaint_type` and `churn_status` belong strictly to their allowed lists in Section 2 (checked after cleaning).

---

## 6. Generator Rules (Enforced by Member 6)

### 6.1 `plan_changes_6m` is a rolling 180-day counter
The generator keeps the list of `(change_date, from_plan, to_plan)` events per customer. On every new snapshot at `event_timestamp`:
1. Drop any plan change where `event_timestamp - change_date > 180 days`.
2. Recompute `plan_changes_6m` as the number of remaining changes.
3. Set `last_plan_changed_from`, `last_plan_changed_to` and `last_plan_change_type` from the **most recent** remaining change. If none remain, set `from` and `to` to `null` and `type` to `"none"`.

### 6.2 Time
Use real UTC time for `event_timestamp` and backdate plan history when a customer is created (see Section 1).

### 6.3 Churn lifecycle
A churned customer sends **exactly one final snapshot** with `churn_status = "Yes"` and `churn_date` equal to the date of that snapshot's `event_timestamp`. After that, the generator sends **no more snapshots** for that customer.

### 6.4 Fault injection (`--inject-errors`)
Injected errors must be ones that **cleaning cannot repair**. Examples, by rule:

| Rule | Example of an injected error |
|---|---|
| 1 | Broken JSON, a missing field, a number sent as a string, a negative count |
| 2 | Wrong `monthly_charges` for the plan |
| 3 | `cs_calls_last_7d` greater than `cs_calls_last_30d` |
| 4 | A complaint type with `cs_calls_last_30d = 0` |
| 5 | Inconsistent plan-change fields (for example type `upgrade` but `to < from`) |
| 6 | `purchase_frequency = 3` with `days_since_last_purchase = null` |
| 7 | `churn_status = "No"` with a `churn_date`, or a `churn_date` in the future |
| 8 | A value outside the list, such as gender `"X"` or location `"Paris"` |

Wrong casing or extra spaces are **not** useful fault injections, because NiFi cleans them.

---

## 7. NiFi Enrichment Fields (Added to `churn.clean`)

NiFi appends these **6 fields** to every valid message. Boolean flags use JSON booleans (`true` / `false`).

| Field | Type | Exact Derivation Rule | Allowed Output Values |
|---|---|---|---|
| `age_group` | string | `age < 25` → `"under_25"`<br>`25 <= age <= 40` → `"25_40"`<br>`41 <= age <= 60` → `"41_60"`<br>`age > 60` → `"over_60"` | `"under_25"`, `"25_40"`, `"41_60"`, `"over_60"` |
| `tenure_group` | string | `tenure_months <= 6` → `"new"`<br>`7 <= tenure_months <= 24` → `"regular"`<br>`tenure_months > 24` → `"loyal"` | `"new"`, `"regular"`, `"loyal"` |
| `purchase_segment` | string | `purchase_frequency == 0` → `"inactive"`<br>`1 <= purchase_frequency <= 2` → `"occasional"`<br>`purchase_frequency >= 3` → `"frequent"` | `"inactive"`, `"occasional"`, `"frequent"` |
| `downgrade_flag` | boolean | `last_plan_change_type == "downgrade"` | `true`, `false` |
| `frequent_plan_changer_flag` | boolean | `plan_changes_6m >= 2` | `true`, `false` |
| `high_complaint_flag` | boolean | `cs_calls_last_7d >= 3` | `true`, `false` |

---

## 8. Dead-Letter Queue Schema (`churn.dlq`)

Every message in `churn.dlq` has these three fields:

| Field | Type | Description |
|---|---|---|
| `error_reason` | string | Code of the **first** failed rule (table below) |
| `failed_at_timestamp` | string | ISO 8601 UTC time when NiFi rejected the record |
| `raw_payload` | string or null | `null` if the message was valid JSON. If the message could not be parsed, the original text exactly as received |

If the message was valid JSON, all its original fields are preserved next to these three fields. If it could not be parsed, only these three fields exist (the Kafka key still holds the `customer_id`).

| Code | Meaning |
|---|---|
| `RULE_1_INVALID_JSON` | Message is not valid JSON |
| `RULE_1_MISSING_OR_TYPE` | Missing required field, wrong type or negative value |
| `RULE_2_PLAN_PRICE_MISMATCH` | Speed or price does not match the plan |
| `RULE_3_CS_CALLS_WINDOW` | 7-day calls greater than 30-day calls |
| `RULE_4_ZERO_CALL_COMPLAINT` | Complaint type with zero calls |
| `RULE_5_PLAN_CHANGE` | Plan-change fields inconsistent |
| `RULE_6_PURCHASES` | Purchase fields inconsistent |
| `RULE_7_CHURN_DATE` | Churn status and date inconsistent |
| `RULE_8_INVALID_ENUM` | Value outside its allowed list |

**Retention:** messages in `churn.dlq` are kept **without a time limit** (`retention.ms=-1`) and nothing consumes them for now. They stay there to be read later, and the team decides what to do with them.

---

## 9. Customer State Rule (Spark and Database)

The database keeps **one row per customer** with the latest snapshot. A new snapshot overwrites the stored row **only if its `event_timestamp` is strictly newer** than the stored one. Late or duplicate snapshots are ignored, so older data never replaces fresh data. A customer whose latest snapshot has `churn_status = "Yes"` stays in the table as churned.

---

## 10. Sample Messages

### 10.1 Valid Enriched Message (`churn.clean`)
```json
{
  "customer_id": "C10023",
  "age": 34,
  "gender": "F",
  "location": "Alexandria",
  "income_level": "medium",
  "tenure_months": 18,
  "calls_count": 120,
  "messages_count": 300,
  "data_usage_mb": 5400.0,
  "plan_id": 3,
  "plan_speed_mbps": 70,
  "monthly_charges": 400.0,
  "payment_method": "credit_card",
  "last_plan_changed_from": 4,
  "last_plan_changed_to": 3,
  "last_plan_change_type": "downgrade",
  "plan_changes_6m": 2,
  "cs_calls_last_7d": 3,
  "cs_calls_last_30d": 5,
  "last_complaint_type": "slow_internet",
  "purchase_frequency": 2,
  "days_since_last_purchase": 21,
  "churn_status": "No",
  "churn_date": null,
  "event_timestamp": "2026-11-14T10:30:00Z",
  "age_group": "25_40",
  "tenure_group": "regular",
  "purchase_segment": "occasional",
  "downgrade_flag": true,
  "frequent_plan_changer_flag": true,
  "high_complaint_flag": true
}
```

### 10.2 Invalid Message, Rule 2 (`churn.dlq`)
```json
{
  "customer_id": "C10099",
  "age": 29,
  "gender": "M",
  "location": "Cairo",
  "income_level": "high",
  "tenure_months": 12,
  "calls_count": 80,
  "messages_count": 100,
  "data_usage_mb": 3200.0,
  "plan_id": 4,
  "plan_speed_mbps": 100,
  "monthly_charges": 200.0,
  "payment_method": "cash",
  "last_plan_changed_from": null,
  "last_plan_changed_to": null,
  "last_plan_change_type": "none",
  "plan_changes_6m": 0,
  "cs_calls_last_7d": 1,
  "cs_calls_last_30d": 2,
  "last_complaint_type": "billing_issue",
  "purchase_frequency": 0,
  "days_since_last_purchase": null,
  "churn_status": "No",
  "churn_date": null,
  "event_timestamp": "2026-11-14T10:31:00Z",
  "error_reason": "RULE_2_PLAN_PRICE_MISMATCH",
  "failed_at_timestamp": "2026-11-14T10:31:02Z",
  "raw_payload": null
}
```

### 10.3 Unparseable Message, Rule 1 (`churn.dlq`)
```json
{
  "error_reason": "RULE_1_INVALID_JSON",
  "failed_at_timestamp": "2026-11-14T10:32:02Z",
  "raw_payload": "{\"customer_id\": \"C10100\", \"age\": 41, \"gender\": "
}
```

---

## 11. Change Log

| Version | Change Summary |
|---|---|
| **v1.0** | Initial contract with `plan_changes_count` (lifetime running counter). |
| **v1.1** | Replaced `plan_changes_count` with `plan_changes_6m` (rolling 6-month window); limited `last_plan_change*` fields to 6 months; defined time windows and purchase and plan-change validation rules. |
| **v1.2** | Exact cutoffs and output values for the 6 enrichment fields; added the `churn.dlq` schema; standardized allowed Egyptian cities. |
| **v1.3** | Real-time `event_timestamp` with backdated plan history; churn lifecycle (one final snapshot) and `churn_date` not after the event date; defined cleaning limits and processing order; rules evaluated in order with error codes; invalid JSON handled through `raw_payload`; numeric price comparison; clearer `age` ranges; `age_group` labels changed to `under_25`, `25_40`, `41_60`, `over_60`; added the customer state rule (newest timestamp wins); added the plan catalog master file. |
| **v1.3 note** | 2026-11-18: no change to fields or rules. Added the flow note at the top and the `churn.dlq` retention rule (Section 8). |
