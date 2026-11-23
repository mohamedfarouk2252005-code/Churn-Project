# 📜 Data Contract: Customer Snapshot (v2.3)

**Owner:** Member 1 (Leader)  |  **Dictionary maintained by:** Member 6  |  **Topics:** `churn.raw`, `churn.clean`, `churn.dlq` (Key = `customer_id`)

Each Kafka message is a **snapshot of one customer**, serialized in UTF-8 JSON. It is built from **one row of `churn_raw.csv`** plus an `event_timestamp` added by the producer. This file is the **single source of truth** for field names, types and rules. Any change must be approved by Member 1 and announced to the entire team the same day.

> **Flow note (2026-11-22):** the selected dataset is `churn_raw.csv` (California telecom customers, Q3 2025, 200,000 rows, 58 columns). This version replaces the generated Egyptian ISP field list and rules from v1.3. The producer sends **52 of the 58 columns plus `event_timestamp`**. It performs schema-guided JSON type serialization only; it does not clean or repair values. NiFi performs formatting-only cleaning and validation (6 ordered rules), but **does not enrich**. Spark creates all derived fields and risk flags (Section 7). The measured facts belong in `docs/dataset_profile.md`, which must be generated from the exact same CSV export and use the trend formula in Section 7.

---

## 1. Time Windows and Time Semantics

| Field Group | Window |
|---|---|
| `calls_1m`, `sms_1m`, `purchases_1m`, `data_usage_gb_1m`, `customer_service_calls_1m` | Last month |
| `calls_3m`, `sms_3m`, `purchases_3m`, `data_usage_gb_3m`, `customer_service_calls_3m` | Last 3 months (**includes** the last month) |
| `avg_monthly_gb_download` | Monthly average. `data_usage_gb_3m` is always `3 × avg_monthly_gb_download` |
| `tenure_in_months`, `total_charges`, `total_refunds`, `total_extra_data_charges`, `total_long_distance_charges`, `total_revenue` | Since the customer joined |
| `churn_date` | A date between `2025-07-01` and `2025-09-30` (Q3 2025) |

**Time semantics:**
- The file has **no event time**. `event_timestamp` is added by the producer and records the **real UTC ingestion/publish time**. It is not a source event timestamp, is not a simulated customer-behaviour time, and is not related to `churn_date`. Replay rate and producer execution time therefore affect its values.
- The source does not state its reference date. The contract assumes every value describes the customer at the end of Q3 2025.
- There is **one row per customer**, so the file does not provide a sequence of historical customer snapshots. The only built-in behaviour-over-time comparison is between the 1-month and 3-month aggregates inside the same row. Because the 3-month value includes the 1-month value, the **two months before the last month** are `x_3m − x_1m` (see Section 7).

**Interpretation:** an upsert ordered by `event_timestamp` chooses the latest received copy. It must not be described as choosing the latest historical customer event or proving that a customer state changed over time.

---

## 2. Raw Fields (`churn.raw`)

Fields marked 🔒 are **evaluation only**: they are streamed and stored, but Spark never reads them to compute derived fields or risk flags. Fields are listed in the order of the file.

| Field | Type | Allowed Values / Rule | Meaning |
|---|---|---|---|
| `customer_id` | string | `C` + 9 digits (`^C\d{9}$`), e.g. `C000000001`. Empty in 5,009 rows of the file (fails Rule 2) | Unique customer ID (Kafka message key when non-empty; null Kafka key when the source value is empty) |
| `gender` | string | `Male`, `Female` (after cleaning; the file also has `M`, `F`, `male`, `FEMALE`, `Male `) | Customer gender |
| `age` | int or null | `18` to `100`; `null` is allowed (35% of rows) | Customer age in years |
| `under_30` | string | `Yes`, `No`; `Yes` if and only if `age < 30` (when `age` is known) | Customer is under 30 |
| `senior_citizen` | string | `Yes`, `No`; `Yes` if and only if `age >= 65` (when `age` is known) | Customer is 65 or older |
| `married` | string | `Yes`, `No` | Customer is married |
| `dependents` | string | `Yes`, `No`; `Yes` if and only if `number_of_dependents >= 1` | Customer has dependents |
| `number_of_dependents` | int | `>= 0` | Number of dependents |
| `city` | string | Non-empty text. No allowed list (1,106 different cities in the file) | City in California |
| `referred_a_friend` | string | `Yes`, `No`; `Yes` if and only if `number_of_referrals >= 1` | Customer has referred a friend |
| `number_of_referrals` | int | `>= 0` | Number of referrals |
| `tenure_in_months` | int | `0` to `72` | Months since the customer joined |
| `offer` | string or null | `Offer A` to `Offer E`; `null` = no offer (55% of rows) | Marketing offer on the account |
| `phone_service` | string | `Yes`, `No` | Customer has phone service |
| `avg_monthly_long_distance_charges` | number | `>= 0`; `0` if `phone_service = "No"` | Average monthly long-distance charges |
| `multiple_lines` | string | `Yes`, `No`; `"No"` if `phone_service = "No"` | Customer has more than one phone line |
| `internet_service` | string | `Yes`, `No` | Customer has internet service |
| `internet_type` | string or null | `DSL`, `Cable`, `Fiber Optic`; `null` if and only if `internet_service = "No"` | Type of internet connection |
| `avg_monthly_gb_download` | int | `>= 0`; `0` if `internet_service = "No"` | Average monthly download (GB) |
| `premium_tech_support` | string | `Yes`, `No`; `"No"` if `internet_service = "No"` | Has the premium tech support add-on |
| `streaming_tv` | string | `Yes`, `No`; `"No"` if `internet_service = "No"` | Uses the streaming TV service |
| `streaming_movies` | string | `Yes`, `No`; `"No"` if `internet_service = "No"` | Uses the streaming movies service |
| `streaming_music` | string | `Yes`, `No`; `"No"` if `internet_service = "No"` | Uses the streaming music service |
| `unlimited_data` | string | `Yes`, `No`; `"No"` if `internet_service = "No"` | Has an unlimited data plan |
| `contract` | string | `Month-to-Month`, `One Year`, `Two Year` | Contract type |
| `paperless_billing` | string | `Yes`, `No` | Customer uses paperless billing |
| `payment_method` | string | `Bank Withdrawal`, `Credit Card`, `Mailed Check` | Billing payment method |
| `monthly_charge` | number | `> 0` and `<= 200` (valid values in the file: 16.51 to 129.71) | Current monthly charge (currency not stated by the source, see Section 11) |
| `total_charges` | number or null | `>= 0`; `null` is allowed (5% of rows) | Total charges since the customer joined |
| `total_refunds` | number | `>= 0` | Total refunds since the customer joined |
| `total_extra_data_charges` | int | `>= 0` | Total extra data charges |
| `total_long_distance_charges` | number | `>= 0` | Total long-distance charges |
| `total_revenue` | number | `>= 0` | Total revenue from the customer (formula in Rule 5) |
| `satisfaction_score` 🔒 | int | `1` to `5` | Customer satisfaction score |
| `customer_status` 🔒 | string | `Stayed`, `Churned`, `Joined` | Status of the customer in the quarter |
| `churn_label` 🔒 | string | `Yes`, `No`; `Yes` if and only if `customer_status = "Churned"` | Whether the customer churned |
| `churn_score` 🔒 | int | `0` to `100` (values in the file: 5 to 96) | The dataset's own churn score (higher = more likely to churn) |
| `cltv` | int | `>= 0` (values in the file: 2,003 to 6,500) | Customer lifetime value, as defined by the source |
| `churn_category` 🔒 | string or null | `Competitor`, `Attitude`, `Dissatisfaction`, `Price`, `Other`; `null` if and only if `customer_status != "Churned"` | Category of the churn reason |
| `churn_reason` 🔒 | string or null | One of the 20 reasons in Section 3, matching its category; `null` if and only if `customer_status != "Churned"` | Reason the customer left |
| `customer_service_calls_1m` | int | `>= 0` | Customer service calls in the last month |
| `customer_service_calls_3m` | int | `>= 0`; `>= customer_service_calls_1m` | Customer service calls in the last 3 months |
| `income` | int | `>= 0` | Customer income (unit not stated by the source) |
| `churn_date` 🔒 | string or null | `YYYY-MM-DD` after cleaning, between `2025-07-01` and `2025-09-30`; `null` if and only if `customer_status != "Churned"` | Date the customer churned |
| `calls_1m` | int | `>= 0` | Voice calls in the last month |
| `calls_3m` | int | `>= 0`; `>= calls_1m` | Voice calls in the last 3 months |
| `sms_1m` | int | `>= 0` | SMS messages in the last month |
| `sms_3m` | int | `>= 0`; `>= sms_1m` | SMS messages in the last 3 months |
| `purchases_1m` | int | `>= 0` | Add-on or top-up purchases in the last month |
| `purchases_3m` | int | `>= 0`; `>= purchases_1m` | Add-on or top-up purchases in the last 3 months |
| `data_usage_gb_1m` | number | `>= 0` | Data used in the last month (GB) |
| `data_usage_gb_3m` | int | `>= 0`; `= 3 × avg_monthly_gb_download` | Data used in the last 3 months (GB) |
| `event_timestamp` | string | ISO 8601 UTC (`YYYY-MM-DDTHH:mm:ssZ`). **Not in the file** | When the producer sent the row |

**Columns of the file that are not sent:** `country`, `state`, `quarter`, `zip_code`, `population`, `churn_value` (reasons in Section 6.1).

---

## 3. Reference Values

**Offers:** `Offer A`, `Offer B`, `Offer C`, `Offer D`, `Offer E` (`null` = no offer).

**Churn categories and reasons** (`churn_reason` must belong to its `churn_category`):

| `churn_category` | Allowed `churn_reason` values |
|---|---|
| `Competitor` | `Competitor had better devices`, `Competitor made better offer`, `Competitor offered higher download speeds`, `Competitor offered more data` |
| `Attitude` | `Attitude of service provider`, `Attitude of support person` |
| `Dissatisfaction` | `Lack of self-service on Website`, `Limited range of services`, `Network reliability`, `Poor expertise of online support`, `Poor expertise of phone support`, `Product dissatisfaction`, `Service dissatisfaction` |
| `Price` | `Extra data charges`, `Lack of affordable download/upload speed`, `Long distance charges`, `Price too high` |
| `Other` | `Deceased`, `Don't know`, `Moved`, `Poor expertise of online support` |

- `Poor expertise of online support` appears under **both** `Dissatisfaction` (385 rows) and `Other` (477 rows) in the file. The contract accepts both (see Section 11, D9).
- **Master:** this section. If NiFi uses a lookup table, keep one copy in `reference/churn_reasons.csv` and let `scripts/verify_pipeline.py` check that it matches.
- `reference/plan_catalog.csv`, `nifi/lookup_tables/plan_catalog.csv` and `sql/04_seed_plans.sql` belong to v1.3 (plans) and are retired.

---

## 4. Processing Order and Cleaning Limits (NiFi)

**Order:** Consume `churn.raw` → **Clean** → **Validate** (Rules 1 to 6) → Publish to `churn.clean` (valid) or `churn.dlq` (invalid). There is **no enrichment step** in NiFi.

Cleaning only fixes **formatting**. It is allowed to:
- Trim leading and trailing spaces in strings (`"Male "` → `"Male"`).
- Map gender words, ignoring letter case: `M`, `male`, `Male` → `Male`; `F`, `FEMALE`, `Female` → `Female`.
- Fix the letter case of other enum values (none are wrong in this file, the rule stays for safety).
- Convert `churn_date` to `YYYY-MM-DD` with exactly this rule:
  - `dd/mm/yyyy` with a **two-digit day and a two-digit month** (`28/09/2025`) → day first.
  - any other `d/m/yyyy` pattern (`7/18/2025`, `9/4/2025`) → **month first**.
  - already `YYYY-MM-DD` → unchanged.
- Convert an empty string `""` to `null` in the 7 nullable fields: `age`, `offer`, `internet_type`, `total_charges`, `churn_date`, `churn_category`, `churn_reason`.

Cleaning must **never**:
- Change a number or a data type (a number sent as a string fails Rule 1).
- Replace an impossible value (age `999`, tenure `-5`, `monthly_charge` `9999`, `satisfaction_score` `99`). Those must reach `churn.dlq`.
- Fill in a missing `age` or `total_charges` (for example by calculating `monthly_charge × tenure_in_months`).
- Guess an ambiguous date. Applying the fixed parsing rule to the current CSV identifies 1,488 rows whose parsed `churn_date` falls outside Q3. All 1,488 rows are labelled `Churned`. Thirty-nine also have an empty `customer_id` and fail the ID check first, so 1,449 are first-failure `RULE_2_BAD_DATE` records. A comparison with an earlier export had found 320 additional day/month mismatches, but those 320 cannot be identified or reproduced from the current CSV alone. These are export-format issues, not evidence that the affected customers are invalid (see Section 11, D4).
- Correct a value that is not in its allowed list.

This keeps errors visible: anything cleaning cannot repair must reach `churn.dlq`.

---

## 5. Validation Rules (Enforced by Member 2 in NiFi)

A record must pass **all 6 rules** to be routed to `churn.clean`. Rules are evaluated **in order 1 to 6**, and the code of the **first failed rule** is stored in `error_reason` (see Section 8).

1. **Parsing, presence and types:**
   - The message is valid JSON.
   - The JSON message contains all 53 contracted fields.
   - All fields are non-null, except the 7 nullable fields (Section 4). A missing field is not represented as `null`; it fails this rule. `customer_id` is checked by Rule 2.
   - Numeric fields are JSON numbers, not strings. Integer fields must be whole numbers. String fields are strings. `city` must contain non-empty text after trimming.
2. **Identity and dates:**
   - `customer_id` is non-empty and matches `^C\d{9}$`.
   - `event_timestamp` is a valid ISO 8601 UTC timestamp.
   - If `churn_date` is not null, it is a valid `YYYY-MM-DD` date between `2025-07-01` and `2025-09-30` (checked after cleaning).
3. **Ranges:**
   - `age` (when not null) is between `18` and `100`.
   - `tenure_in_months` is between `0` and `72`.
   - `monthly_charge` is greater than `0` and at most `200`.
   - `satisfaction_score` is between `1` and `5`. `churn_score` is between `0` and `100`.
   - Every other count and amount in Section 2 is `>= 0`.
4. **Allowed enum values:** `gender`, every `Yes`/`No` field, `offer`, `internet_type`, `contract`, `payment_method`, `customer_status`, `churn_label`, `churn_category` and `churn_reason` belong strictly to their allowed lists in Sections 2 and 3 (checked after cleaning).
5. **Cross-field consistency:**
   - `total_charges` (when not null and `tenure_in_months >= 1`) equals `monthly_charge × tenure_in_months`, with a tolerance of `0.02`. When `tenure_in_months = 0`, `total_charges` has no fixed relation to the monthly charge and is not checked.
   - `total_revenue` (when `total_charges` is not null) equals `total_charges − total_refunds + total_extra_data_charges + total_long_distance_charges`, with a tolerance of `0.02`.
   - For each pair, the 3-month value is at least the 1-month value: `calls`, `sms`, `purchases`, `data_usage_gb`, `customer_service_calls`.
   - `data_usage_gb_3m == 3 × avg_monthly_gb_download`.
   - If `internet_service = "No"`: `internet_type` is `null`; `premium_tech_support`, `streaming_tv`, `streaming_movies`, `streaming_music` and `unlimited_data` are `"No"`; `avg_monthly_gb_download`, `data_usage_gb_1m` and `total_extra_data_charges` are `0`. If `internet_service = "Yes"`, `internet_type` is not null.
   - If `phone_service = "No"`: `multiple_lines` is `"No"`, and `avg_monthly_long_distance_charges` and `total_long_distance_charges` are `0`.
   - `dependents = "Yes"` if and only if `number_of_dependents >= 1`. `referred_a_friend = "Yes"` if and only if `number_of_referrals >= 1`.
   - If `age` is not null: `under_30 = "Yes"` if and only if `age < 30`, and `senior_citizen = "Yes"` if and only if `age >= 65`.
6. **Churn consistency:**
   - `customer_status = "Churned"` if and only if `churn_label = "Yes"`.
   - `churn_date`, `churn_category` and `churn_reason` are non-null if and only if `customer_status = "Churned"`.
   - `churn_reason` belongs to its `churn_category` (Section 3).
   - `churn_date` is **not later than the date of `event_timestamp`**. Since `event_timestamp` is an ingestion timestamp rather than a source event time, this is only a check that the churn date is not in the future relative to ingestion.

### Expected results on `churn_raw.csv`

These numbers come from applying Sections 4 and 5 to the file, and `scripts/verify_pipeline.py` should check them. The producer sends all 200,000 rows, including the duplicates.

| Result | Rows |
|---|---|
| Sent to `churn.raw` | 200,000 |
| `RULE_2_BAD_ID` (empty `customer_id`) | 5,009 |
| `RULE_2_BAD_DATE` (`churn_date` outside Q3 after cleaning, not counting rows already rejected) | 1,449 |
| `RULE_3_OUT_OF_RANGE` (not counting rows already rejected) | 16,010 |
| Rules 1, 4, 5 and 6 | 0 (the file never breaks them, so only fault injection tests them) |
| **Total in `churn.dlq`** | **22,468 (11.2%)** |
| **Total in `churn.clean`** | **177,532**, of which 173,099 are different customers and 4,433 are exact duplicates |

Rule 3 failures by cause, counted over all 200,000 rows (a row can have more than one): `age` 3,249, `tenure_in_months` 3,792, `monthly_charge` 4,989, `satisfaction_score` 5,005.

**Churn-date impact:** all 1,488 current-file rows parsed outside Q3 have `customer_status = "Churned"`. Thirty-nine also fail the empty-ID check, so 1,449 are first rejected as `RULE_2_BAD_DATE`. The expected current-state table contains 44,739 churned customers out of 173,099 (25.8%); after exact-duplicate removal, the source file contains 51,526 churned rows out of 195,000 (26.4%). The final state table is filtered by every validation rule, so the rates are not directly comparable. The date rule itself removes churn-labelled rows because of the export-format issue; it is not evidence of invalid customer behaviour.

---

## 6. Producer Rules (Enforced by Member 6)

### 6.1 Fields sent
The producer sends **52 columns of the file plus `event_timestamp`**, one JSON message per row. It reads columns **by header name**. It must stop with an error if a column of Section 2 is missing or if a header is empty (a previous export had 5 columns with an empty header).

Not sent (6 columns):

| Column | Reason |
|---|---|
| `country`, `state`, `quarter` | One value in all rows: `United States`, `California`, `Q3`. |
| `churn_value` | A copy of `churn_label` as `1` / `0`. |
| `zip_code`, `population` | Not used by the scoring or the dashboard. They can be added later (`zip_code` as a string). |

### 6.2 JSON serialization and preservation of source values

CSV readers return text tokens, but the Kafka contract requires typed JSON values. The producer must therefore perform **schema-guided serialization only**:

- Fields typed as `int` are emitted as JSON integers when the token is a valid whole number.
- Fields typed as `number` are emitted as JSON numbers when the token is numerically parseable.
- Fields typed as `string` remain JSON strings.
- Numeric values outside the allowed range (for example, `age = 999` or `monthly_charge = 9999`) remain JSON numbers. NiFi must reject them under Rule 3, not have the producer repair them.
- If a token in a numeric field cannot be parsed as a number, preserve it as a JSON string so Rule 1 can reject the wrong type. Do not replace it with `null` or a default.
- Empty CSV cells are emitted as empty strings (`""`). NiFi converts them to `null` only in the seven nullable fields listed in Section 4. An empty `customer_id` remains an empty string for Rule 2 to reject.
- If a source row is missing a required cell/field, do not invent a value. The message must fail Rule 1 for a missing field.

This type serialization is not data cleaning. The producer must not trim strings, normalize gender spelling, interpret dates, fill missing values, or repair invalid values. Fault-injection records may deliberately use incorrect JSON types or malformed JSON to test Rule 1.

### 6.3 Time and order
Use real UTC wall-clock time for `event_timestamp` when the producer publishes each record, and send rows in **file order**. This timestamp is an ingestion timestamp, not the time the customer's behaviour occurred. The file has no event time to sort by (only the 26% of rows with a `churn_date` have a churn date). Replaying 200,000 rows takes about 5.6 hours at 10 messages per second, 33 minutes at 100 per second, and 3.3 minutes at 1,000 per second.

### 6.4 Churn lifecycle and duplicates
Each customer is already represented by a final-state row in the file. A churned customer is one row with `customer_status = "Churned"`. The producer does not create extra snapshots. The file contains 5,000 **exact duplicate rows**. The producer sends them as they are, each with its own `event_timestamp`; these later timestamps only indicate later publication, not a newer real-world customer snapshot. Section 9 handles them.

When `customer_id` is non-empty, it is used as the Kafka key. When `customer_id` is empty, the Kafka key is null; the empty ID remains in the JSON payload so NiFi can reject it under Rule 2.

### 6.5 Fault injection (`--inject-errors`)
The real file already fills Rules 2 and 3 and never breaks Rules 1, 4, 5 and 6, so injection must cover those. Injected errors must be ones that **cleaning cannot repair**. Examples, by rule:

| Rule | Example of an injected error |
|---|---|
| 1 | Broken JSON, a missing field, a number sent as a string |
| 2 | `customer_id` `"X123"`, a `churn_date` of `2025-13-45` or in 2024 |
| 3 | A negative count, `satisfaction_score` `7` |
| 4 | `contract` `"Annual"`, `payment_method` `"Cash"` |
| 5 | `internet_service` `"No"` with `internet_type` `"Fiber Optic"`; `calls_3m` smaller than `calls_1m` |
| 6 | `churn_label` `"Yes"` with `customer_status` `"Stayed"`; a `churn_date` with `"Stayed"` |

Wrong casing or extra spaces are **not** useful fault injections, because NiFi cleans them.

---

## 7. Derived Fields (Created by Spark, Not in `churn.raw` or `churn.clean`)

Spark adds these fields to every customer row in MySQL. **Spark never reads the 🔒 evaluation-only fields to compute them**, and a test checks this.

| Field | Type | Exact Derivation Rule | Allowed Output Values |
|---|---|---|---|
| `age_group` | string | `age` is null → `"unknown"`<br>`age < 25` → `"under_25"`<br>`25 <= age <= 40` → `"25_40"`<br>`41 <= age <= 60` → `"41_60"`<br>`age > 60` → `"over_60"` | `"unknown"`, `"under_25"`, `"25_40"`, `"41_60"`, `"over_60"` |
| `tenure_group` | string | `tenure_in_months <= 6` → `"new"`<br>`7 <= tenure_in_months <= 24` → `"regular"`<br>`tenure_in_months > 24` → `"loyal"` | `"new"`, `"regular"`, `"loyal"` |
| `calls_trend`, `sms_trend`, `purchases_trend`, `data_usage_trend`, `cs_calls_trend` | number or null | `x_1m / ((x_3m − x_1m) / 2)`, where `x` is `calls`, `sms`, `purchases`, `data_usage_gb` and `customer_service_calls`. `null` if the denominator is `0` | A number `>= 0`, or `null` |
| `risk_score` | int | The sum of the points in `docs/scoring_rules.md` (to be agreed) | An integer `>= 0` |
| `risk_level` | string | The thresholds in `docs/scoring_rules.md` (to be agreed) | Defined in `docs/scoring_rules.md` |
| `risk_reasons` | string | The signals that gave points, in the format defined in `docs/scoring_rules.md` | Defined in `docs/scoring_rules.md` |

**Scoring/evaluation diagnostic:** the seven 🔒 fields are prohibited from feature derivation and risk scoring. The dataset profile also identifies unusually strong associations between some allowed behaviour fields and the label, especially `calls_*` and `sms_*`, with `customer_service_calls_3m` and `purchases_3m` also strongly associated. Before finalizing the points, the team should compare a baseline score with an ablation that excludes `calls_1m`, `calls_3m`, `sms_1m`, and `sms_3m`; it should also report the effect of `customer_service_calls_3m` and `purchases_3m`. These are retrospective diagnostics, not proof of future predictive performance.

---

## 8. Dead-Letter Queue Schema (`churn.dlq`)

Every message in `churn.dlq` has these three fields:

| Field | Type | Description |
|---|---|---|
| `error_reason` | string | Code of the **first** failed rule (table below) |
| `failed_at_timestamp` | string | ISO 8601 UTC time when NiFi rejected the record |
| `raw_payload` | string or null | `null` if the message was valid JSON. If the message could not be parsed, the original text exactly as received |

If the message was valid JSON, all its original fields are preserved next to these three fields, with the values **as the producer sent them** (before cleaning). If it could not be parsed, only these three fields exist. The Kafka key is the `customer_id` when non-empty and is null when the source row has no ID; the payload still retains an empty `customer_id` string for Rule 2 to reject.

| Code | Meaning |
|---|---|
| `RULE_1_INVALID_JSON` | Message is not valid JSON |
| `RULE_1_MISSING_OR_TYPE` | Missing field, null in a required field, or wrong type |
| `RULE_2_BAD_ID` | `customer_id` is empty or does not match `^C\d{9}$` |
| `RULE_2_BAD_DATE` | `event_timestamp` is invalid, or `churn_date` is invalid or outside Q3 2025 |
| `RULE_3_OUT_OF_RANGE` | A value is outside its range |
| `RULE_4_INVALID_ENUM` | Value outside its allowed list |
| `RULE_5_CROSS_FIELD` | Related fields are inconsistent |
| `RULE_6_CHURN_INCONSISTENT` | Churn status, label, date, category or reason are inconsistent |

**Retention:** messages in `churn.dlq` are kept **without a time limit** (`retention.ms=-1`) and nothing consumes them for now. They stay there to be read later, and the team decides what to do with them.

---

## 9. Customer State Rule (Spark and Database)

The database keeps **one row per customer**. An incoming record overwrites the stored row **only if its `event_timestamp` is strictly newer** than the stored timestamp. In this dataset, there is only one source snapshot per customer; repeated rows are exact duplicates. The rule therefore selects the latest received copy and protects the table from late or out-of-order message delivery. It does **not** prove that the selected record is a newer real-world customer state. A customer whose stored row has `customer_status = "Churned"` stays in the table as churned.

- The 4,433 duplicate rows that reach `churn.clean` carry the same source values as their earlier copies and a later ingestion `event_timestamp`, so they may update the row with identical customer data.
- Rows with an empty `customer_id` never reach the database (they stop at Rule 2).
- After a full replay of `churn_raw.csv`, `customers_current_state` must hold **173,099 rows**: 117,194 `Stayed`, 44,739 `Churned` and 11,166 `Joined`.

---

## 10. Sample Messages

### 10.1 Valid Message (`churn.clean`)
```json
{
  "customer_id": "C000000001",
  "gender": "Female",
  "age": 43,
  "under_30": "No",
  "senior_citizen": "No",
  "married": "No",
  "dependents": "No",
  "number_of_dependents": 0,
  "city": "Pasadena",
  "referred_a_friend": "No",
  "number_of_referrals": 0,
  "tenure_in_months": 34,
  "offer": null,
  "phone_service": "Yes",
  "avg_monthly_long_distance_charges": 32.02,
  "multiple_lines": "Yes",
  "internet_service": "Yes",
  "internet_type": "DSL",
  "avg_monthly_gb_download": 3,
  "premium_tech_support": "No",
  "streaming_tv": "No",
  "streaming_movies": "No",
  "streaming_music": "No",
  "unlimited_data": "Yes",
  "contract": "Month-to-Month",
  "paperless_billing": "Yes",
  "payment_method": "Bank Withdrawal",
  "monthly_charge": 58.19,
  "total_charges": 1978.46,
  "total_refunds": 0.0,
  "total_extra_data_charges": 0,
  "total_long_distance_charges": 1088.68,
  "total_revenue": 3067.14,
  "satisfaction_score": 5,
  "customer_status": "Stayed",
  "churn_label": "No",
  "churn_score": 43,
  "cltv": 4454,
  "churn_category": null,
  "churn_reason": null,
  "customer_service_calls_1m": 1,
  "customer_service_calls_3m": 2,
  "income": 3192,
  "churn_date": null,
  "calls_1m": 133,
  "calls_3m": 431,
  "sms_1m": 86,
  "sms_3m": 272,
  "purchases_1m": 0,
  "purchases_3m": 4,
  "data_usage_gb_1m": 2.77,
  "data_usage_gb_3m": 9,
  "event_timestamp": "2026-11-22T10:30:00Z"
}
```

### 10.2 Invalid Message, Rule 3 (`churn.dlq`)
`monthly_charge` is `9999.0`, which is outside `0 < x <= 200`.
```json
{
  "customer_id": "C000000516",
  "gender": "Male",
  "age": 22,
  "under_30": "Yes",
  "senior_citizen": "No",
  "married": "Yes",
  "dependents": "Yes",
  "number_of_dependents": 2,
  "city": "Canby",
  "referred_a_friend": "Yes",
  "number_of_referrals": 2,
  "tenure_in_months": 19,
  "offer": null,
  "phone_service": "No",
  "avg_monthly_long_distance_charges": 0.0,
  "multiple_lines": "No",
  "internet_service": "Yes",
  "internet_type": "DSL",
  "avg_monthly_gb_download": 62,
  "premium_tech_support": "No",
  "streaming_tv": "Yes",
  "streaming_movies": "No",
  "streaming_music": "No",
  "unlimited_data": "Yes",
  "contract": "One Year",
  "paperless_billing": "No",
  "payment_method": "Credit Card",
  "monthly_charge": 9999.0,
  "total_charges": 842.46,
  "total_refunds": 0.0,
  "total_extra_data_charges": 0,
  "total_long_distance_charges": 0.0,
  "total_revenue": 842.46,
  "satisfaction_score": 5,
  "customer_status": "Stayed",
  "churn_label": "No",
  "churn_score": 35,
  "cltv": 4003,
  "churn_category": null,
  "churn_reason": null,
  "customer_service_calls_1m": 1,
  "customer_service_calls_3m": 1,
  "income": 2174,
  "churn_date": null,
  "calls_1m": 128,
  "calls_3m": 395,
  "sms_1m": 78,
  "sms_3m": 251,
  "purchases_1m": 3,
  "purchases_3m": 5,
  "data_usage_gb_1m": 57.2,
  "data_usage_gb_3m": 186,
  "event_timestamp": "2026-11-22T10:30:00Z",
  "error_reason": "RULE_3_OUT_OF_RANGE",
  "failed_at_timestamp": "2026-11-22T10:30:02Z",
  "raw_payload": null
}
```

### 10.3 Unparseable Message, Rule 1 (`churn.dlq`)
```json
{
  "error_reason": "RULE_1_INVALID_JSON",
  "failed_at_timestamp": "2026-11-22T10:31:02Z",
  "raw_payload": "{\"customer_id\": \"C000000200\", \"gender\": \"Male\", \"age\": 41, \"under_30\": "
}
```

### 10.4 Invalid Message, Rule 2 (`churn.dlq`, excerpt)
The producer sent `churn_date` as `11/7/2025`. Cleaning reads it month first (`2025-11-07`), which is outside Q3, so Rule 2 rejects it. The other original fields are kept in the real message and are left out here to save space.
```json
{
  "customer_id": "C000000031",
  "gender": "Female",
  "age": 60,
  "tenure_in_months": 1,
  "monthly_charge": 76.98,
  "customer_status": "Churned",
  "churn_label": "Yes",
  "churn_category": "Attitude",
  "churn_reason": "Attitude of support person",
  "churn_date": "11/7/2025",
  "event_timestamp": "2026-11-22T10:30:00Z",
  "error_reason": "RULE_2_BAD_DATE",
  "failed_at_timestamp": "2026-11-22T10:30:02Z",
  "raw_payload": null
}
```

---

## 11. Change Log and Current Defaults

| Version | Change Summary |
|---|---|
| **v1.0** | Initial contract with `plan_changes_count` (lifetime running counter). |
| **v1.1** | Replaced `plan_changes_count` with `plan_changes_6m` (rolling 6-month window); limited `last_plan_change*` fields to 6 months; defined time windows and purchase and plan-change validation rules. |
| **v1.2** | Exact cutoffs and output values for the 6 enrichment fields; added the `churn.dlq` schema; standardized allowed Egyptian cities. |
| **v1.3** | Real-time `event_timestamp` with backdated plan history; churn lifecycle (one final snapshot) and `churn_date` not after the event date; defined cleaning limits and processing order; rules evaluated in order with error codes; invalid JSON handled through `raw_payload`; numeric price comparison; clearer `age` ranges; `age_group` labels changed to `under_25`, `25_40`, `41_60`, `over_60`; added the customer state rule (newest timestamp wins); added the plan catalog master file. |
| **v1.3 note** | 2026-11-18: no change to fields or rules. Added the flow note at the top and the `churn.dlq` retention rule (Section 8). |
| **v2.0** | New dataset: `churn_raw.csv` (California telecom, Q3 2025) replaces the generated Egyptian schema. Removed plans, the plan catalog, plan changes, complaint type, `income_level`, `location` and the 7-day window. Fields follow the file, with 1-month and 3-month windows. |
| **v2.1** | 52 file columns plus `event_timestamp`; 6 validation rules (identity and dates, and ranges, are separate rules); NiFi no longer enriches, and derived fields and risk flags are created in Spark (Section 7); evaluation-only fields (🔒); tighter ranges that catch the impossible values in the file (age `0`/`150`/`999`, tenure `-5`/`150`/`999`, `monthly_charge` `0`/`-1`/`-50`/`9999`, `satisfaction_score` `0`/`6`/`10`/`99`); `churn_date` cleaning rule for the two date formats; expected results on the file (Section 5); DLQ codes renamed. |
| **v2.2** | Clarified that `event_timestamp` is an ingestion timestamp, not source event time; specified schema-guided producer JSON type serialization without cleaning/repair; specified null Kafka keys for empty customer IDs; clarified missing-field and non-empty-city handling; documented the limits of latest-received-record upserts; added scoring ablation diagnostics for strongly label-associated behaviour fields. |
| **v2.3** | Corrected the date-quality explanation: 1,488 out-of-Q3 rows are detectable from the current CSV and are all churn-labelled; 39 overlap with empty customer IDs, leaving 1,449 first-failure date rejections. The additional 320 day/month mismatches are historical prior-export comparison results, not reproducible from the current CSV. Documented the resulting churn-share effect in the filtered state table. |

### Current defaults (to be confirmed by the team)

| # | Decision | Current default |
|---|---|---|
| D1 | Spelling of `gender` after cleaning | `Male` / `Female` (the file's own spelling). v1.3 used `M` / `F`. |
| D2 | `age` | Nullable. A null age is neither filled nor rejected, and `age_group` is `"unknown"`. If `age` were required, 43% of the rows would go to the DLQ instead of 11.2%. |
| D3 | `total_charges` | Nullable (9,985 rows). Never filled by NiFi. |
| D4 | Churn-date export issue | Strict cleaning rule (Section 4), no guessing. The current CSV itself identifies 1,488 rows whose parsed dates fall outside Q3; all 1,488 are labelled `Churned`. Thirty-nine also have an empty `customer_id` and fail the ID check first, leaving 1,449 first-failure `RULE_2_BAD_DATE` records. A comparison with an earlier export found 320 further day/month mismatches, but those cannot be identified or reproduced from this CSV alone. The detected date failures are an export-format problem, not evidence that the customers are invalid. Re-export the dates from the original source in an unambiguous format if accurate dates are required. |
| D5 | `Joined` customers (12,825 rows, 6.4%) | Stay in the stream. The source sets `churn_label = "No"` for them, so the primary binary evaluation counts them as not churned. Report the `Joined` segment separately to show its effect on the metrics. |
| D6 | Columns not sent | `country`, `state`, `quarter`, `zip_code`, `population`, `churn_value` (Section 6.1). |
| D7 | Evaluation-only fields and label-associated signals | The seven 🔒 fields (`customer_status`, `churn_label`, `churn_date`, `churn_category`, `churn_reason`, `churn_score`, `satisfaction_score`) may be carried/stored for evaluation but must never be used for feature derivation or risk scoring. Some non-🔒 behaviour fields are also unusually strongly associated with the label: `calls_*` and `sms_*` separate groups almost perfectly, while `customer_service_calls_3m` and `purchases_3m` are also strongly associated. Before finalizing scoring, compare a baseline with an ablation excluding `calls_*` and `sms_*`, and report the effect of the other strongly associated fields. All evaluation is retrospective. |
| D8 | Trend fields | A ratio of the last month to the average of the two months before it. `null` when that average is `0`. |
| D9 | `Poor expertise of online support` | Accepted under both `Dissatisfaction` and `Other`. |
| D10 | Currency and units | The source does not state the currency of `monthly_charge` or the unit of `income`. The team assumes US dollars for charges. |
