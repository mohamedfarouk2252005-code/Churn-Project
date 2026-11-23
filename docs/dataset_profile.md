# 📊 Dataset Profile — `churn_raw.csv`

This document records measured characteristics of the selected 58-column dataset export. It is the factual companion to [`data_contract.md`](data_contract.md): the contract defines processing rules; this profile records what was observed in the file.

> **Dataset version:** the 58-column `churn_raw.csv` supplied with this review (200,000 rows). Re-run this profile whenever the CSV export changes. Do not carry measurements from an earlier export into this profile.

**File:** `data/raw/churn_raw.csv`  
**SHA-256:** `f9c6176ab8b91ef8c305bd783d3467a417202a5ebbc466012f84f06b8b74ad11`  
**Rows / columns:** 200,000 / 58

---

## 1. Overview

| Item | Measured value |
|---|---|
| Rows | 200,000 |
| Columns | 58 |
| Exact duplicate rows | 5,000 |
| Rows after removing exact duplicates | 195,000 |
| Distinct non-empty `customer_id` values | 190,132 |
| Rows with empty `customer_id` | 5,009 (2.5%) |
| Customer status in the raw file | `Stayed`: 134,340 (67.2%); `Churned`: 52,835 (26.4%); `Joined`: 12,825 (6.4%) |
| `churn_label` in the raw file | `Yes`: 52,835; `No`: 147,165. `Joined` customers are labelled `No`. |
| Constant columns | `country` = `United States`; `state` = `California`; `quarter` = `Q3` |
| Source event time | None. The producer adds `event_timestamp` when it publishes each row; this is an ingestion timestamp. |
| Date field | `churn_date`, populated for churners only (52,835 rows). |
| Behaviour windows | Last-month and last-three-month totals for calls, SMS, purchases, data usage, and customer-service calls. The three-month total includes the last month. |

The producer sends 52 of the 58 source columns plus `event_timestamp`. The six source columns not sent are `country`, `state`, `quarter`, `churn_value`, `zip_code`, and `population`.

There is no sequence of dated customer snapshots in this file. Behaviour over time can only be approximated by comparing the last-month aggregate with the two-month aggregate preceding it; this is not a true customer event history.

No non-empty `customer_id` is associated with multiple different source rows. The repeated IDs in this export are exact duplicate rows.

---

## 2. Data Quality Facts

Counts in this section refer to the original 200,000-row file unless stated otherwise. Counts for different problems can overlap.

| Fact | Rows / frequency | Pipeline treatment |
|---|---:|---|
| Empty `customer_id` | 5,009 (2.5%) | Rejected by Rule 2 as `RULE_2_BAD_ID`. |
| Exact duplicate rows | 5,000 | Sent as-is. 4,433 duplicate rows remain in `churn.clean`; the database's customer key leaves one row per customer. |
| Non-canonical `gender` values | 10,008 (5.0%) | NiFi formatting cleanup maps `M`, `F`, `male`, `FEMALE`, and `Male ` to `Male` / `Female`. |
| Missing `age` | 70,046 (35.0%) | Allowed as `null`; not imputed. |
| Invalid `age` values (`-1`, `0`, `150`, `999`) | 3,249 | Rejected by Rule 3. The observed valid range is 18–83. |
| Invalid `tenure_in_months` values (`-5`, `150`, `999`) | 3,792 | Rejected by Rule 3. The observed valid range is 0–72. |
| Invalid `monthly_charge` values (`-50`, `-1`, `0`, `9999`) | 4,989 | Rejected by Rule 3. The observed valid range is 16.51–129.71. |
| Invalid `satisfaction_score` values (`0`, `6`, `10`, `99`) | 5,005 | Rejected by Rule 3. The valid score range is 1–5. |
| Missing `total_charges` | 9,985 (5.0%) | Allowed as `null`; not calculated or filled. |
| Missing `offer` | 110,254 (55.1%) | Allowed as `null`; represents no offer. |
| Missing `internet_type` | 43,398 (21.7%) | Allowed as `null`; missing exactly when `internet_service = "No"`. |
| Missing `churn_date`, `churn_category`, and `churn_reason` | 147,165 rows for each field | Allowed as `null`; these fields are empty exactly when `churn_label = "No"`. |

### Churn-date export issue

The current CSV contains 52,835 non-empty `churn_date` values, all for customers labelled `Churned`. Applying the contract's fixed parsing rule identifies **1,488 rows whose parsed dates fall outside Q3 2025**. All 1,488 are labelled `Churned`.

Of those 1,488 rows, **39 also have an empty `customer_id`** and fail the ID check first. Therefore:

- `RULE_2_BAD_ID`: 5,009 rows.
- `RULE_2_BAD_DATE`: 1,449 first-failure rows.
- Total rows with an out-of-Q3 parsed date: 1,488.

These are an **export-format problem**, not evidence that the affected customers or their behaviour are invalid. Re-exporting the dates from the original source in an unambiguous format is the appropriate correction.

A comparison with an earlier export found **320 additional day/month mismatches**. Those 320 were discovered by comparing exports; they cannot be identified or reproduced from the current CSV alone. Consequently, the combined historical figure of 1,808 is not a measurement that can be independently regenerated from this file. The current-file result is 1,488 out-of-Q3 rows.

---

## 3. Expected Result of Applying Contract v2.3

The following counts are the expected routing results when the producer sends all 200,000 rows and NiFi applies the contract's six rules in order. They are source-derived expectations, not a claim that the deployed pipeline has already been executed.

| Result | Rows |
|---|---:|
| Sent to `churn.raw` | 200,000 |
| First failure: Rule 2, empty `customer_id` (`RULE_2_BAD_ID`) | 5,009 |
| First failure: Rule 2, date outside Q3 (`RULE_2_BAD_DATE`) | 1,449 |
| First failure: Rule 3, out of range (`RULE_3_OUT_OF_RANGE`) | 16,010 |
| First failure: Rules 1, 4, 5, or 6 | 0 |
| **Total expected in `churn.dlq`** | **22,468 (11.2%)** |
| **Total expected in `churn.clean`** | **177,532** |
| Exact duplicate rows remaining in `churn.clean` | 4,433 |
| **Distinct customers expected in `customers_current_state`** | **173,099** |

Expected customer-status counts in `customers_current_state`:

| Status | Customers |
|---|---:|
| `Stayed` | 117,194 |
| `Churned` | 44,739 |
| `Joined` | 11,166 |
| **Total** | **173,099** |

Rule 3 cause counts, counted across the full source file, are `age`: 3,249; `tenure_in_months`: 3,792; `monthly_charge`: 4,989; and `satisfaction_score`: 5,005. These cause counts overlap. The number of rows first rejected by Rule 3 after the earlier Rule 2 checks is 16,010.

### Effect on the observed churn share

After removing exact duplicates, the source contains 51,526 churned rows out of 195,000 (26.4%). The expected current-state table contains 44,739 churned customers out of 173,099 (25.8%).

The current-file date rule removes churn-labelled rows: 1,449 first-failure date records are rejected by `RULE_2_BAD_DATE`, and 39 further out-of-Q3 rows are already rejected for empty customer IDs. Other validation rules also remove rows, so the source and current-state percentages describe different populations. The 25.8% current-state figure must not be presented as the unfiltered churn prevalence of the source file.

The real file is expected not to trigger Rules 1, 4, 5, or 6. The producer's `--inject-errors` option is needed to test those validation paths.

---

## 4. Does Behaviour Differ Between Churned and Other Customers?

The measurements below use the 195,000 rows remaining after removing the 5,000 exact duplicate rows. `Churned` means `churn_label = "Yes"`; `Other` means `churn_label = "No"` and includes `Stayed` and `Joined`.

| Signal | Measured result |
|---|---|
| `calls_1m` | Churners: 45–119 (median 80); other customers: 90–201 (median 140). The rule `calls_1m < 100` has 99.95% precision and 98.3% recall on this file. |
| `sms_1m` | Churners: 13–70 (median 40); other customers: 52–133 (median 90). |
| Customer-service calls in the last month | Churn rate: 6.9% with 0 calls; 16.6% with 1; 34.4% with 2; 58.1% with 3; 92.5% with 5 or more. |
| Purchases in the last 3 months | 17,149 customers have no purchases; 90.8% of that group are labelled `Churned`. |
| Data-usage trend | Median 0.73 for churners and 1.03 for other customers, using the contract's last-month-versus-prior-two-months formula and excluding zero denominators. |
| Calls and SMS trends | The median trend ratio is 1.00 for both churners and other customers. These two signals show a strong level difference, not a clear difference in the median trend ratio. |
| `contract` | Churn rate: 45.6% for `Month-to-Month`; 10.8% for `One Year`; 2.6% for `Two Year`. |
| `offer` | Churn rate: 52.6% for `Offer E`; 7.1% for `Offer A`. |
| `internet_type` | Churn rate: 40.7% for `Fiber Optic`; 25.5% for `Cable`; 18.4% for `DSL`; 7.3% with no internet type. |
| `satisfaction_score` (valid values only) | Scores 1 and 2: 100% churn; score 3: 16.0%; scores 4 and 5: 0%. This field is evaluation-only and must not be used in risk scoring. |
| `churn_score` (dataset-provided score) | AUC 0.942. The rule `churn_score >= 60` has 50.9% precision and 100% recall; it flags 51.9% of deduplicated rows. This field is evaluation-only and is shown only as a benchmark. |

### Interpretation and evaluation limits

The behavioural fields are unusually strongly associated with the final churn labels. In particular, calls and SMS separate the two groups almost perfectly. `customer_service_calls_3m` and `purchases_3m` are also strongly associated with the label.

These associations may reflect label-dependent data generation or other target leakage. The current file cannot establish the cause. Therefore:

- The evaluation is retrospective: the final churn label is already present in the same customer snapshot.
- High precision, recall, or AUC on this dataset does not establish that the pipeline can predict a future cancellation.
- The seven evaluation-only fields (`customer_status`, `churn_label`, `churn_date`, `churn_category`, `churn_reason`, `churn_score`, and `satisfaction_score`) must never be used for feature derivation or risk scoring.
- Before agreeing on the final scoring points, compare a baseline with an ablation that excludes `calls_1m`, `calls_3m`, `sms_1m`, and `sms_3m`. Also report the effect of `customer_service_calls_3m` and `purchases_3m`.

---

## 5. Method

- Source-row and data-quality counts were measured on all 200,000 rows, unless a section explicitly says exact duplicates were removed.
- Behaviour comparisons and the reported churn rate of 26.4% use the 195,000 rows remaining after exact-duplicate removal.
- `Joined` customers have `churn_label = "No"` and are included in the "Other" group. Their segment should also be reported separately in dashboard evaluation.
- The date results use the fixed parsing rule in the data contract: when both day and month are two digits, interpret the date as day-first; otherwise interpret it as month-first. The current CSV itself reveals 1,488 parsed dates outside Q3, all for churn-labelled rows. The extra 320 mismatches come only from a prior-export comparison.
- Trend is calculated as `x_1m / ((x_3m - x_1m) / 2)`, where the 3-month total includes the last month. The denominator represents the average over the two preceding months. A zero denominator produces `null`.
- Precision and recall treat `churn_label = "Yes"` as the positive class. The `calls_1m < 100` rule is reported as a simple descriptive threshold, not a validated predictive model.
- AUC is rank-based. The `churn_score` AUC is a benchmark only because `churn_score` is evaluation-only.
