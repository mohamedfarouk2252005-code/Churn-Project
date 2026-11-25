# 📖 Data Dictionary

**Maintained by:** Member 6 (Killy)
**Rules and types:** live in `docs/data_contract.md` — this file adds **business meaning only**.
**Dataset:** `churn_raw.csv` — California telecom customers, Q3 2025, 200,000 rows.
**Topics:** `churn.raw` → `churn.clean` (Key = `customer_id`)

> Fields marked 🔒 are **evaluation-only**: stored and displayed but never used to compute derived fields or risk scores.

---

## Customer Identity

| Field | Business Meaning |
|---|---|
| `customer_id` | Unique identifier assigned to each customer account. Format: letter C followed by 9 digits. |
| `gender` | The customer's gender as recorded at account creation. |
| `age` | Customer's age in years at the end of Q3 2025. Null for ~35% of customers (not collected). |
| `under_30` | Convenience flag: `Yes` if the customer is younger than 30. Derived from `age`. |
| `senior_citizen` | Convenience flag: `Yes` if the customer is 65 or older. Senior citizens have a significantly higher churn rate (40%). |
| `married` | Whether the customer is married. |
| `dependents` | Whether the customer has any dependents (children, elderly relatives, etc.). |
| `number_of_dependents` | The exact count of dependents on the account. |
| `city` | The California city where the customer resides. 1,106 distinct cities in the dataset. |
| `income` | The customer's reported income. Unit not stated by the source; assumed to be US dollars. |

---

## Account & Relationship

| Field | Business Meaning |
|---|---|
| `referred_a_friend` | Whether the customer has ever referred at least one new customer. Loyal customers are more likely to refer. |
| `number_of_referrals` | Total number of successful referrals the customer has made. |
| `tenure_in_months` | How long the customer has been with the company, in months (0–72). Shorter tenure correlates strongly with churn: new customers (0–6 months) churn at 52.6%. |
| `offer` | The promotional offer currently applied to the account (Offer A–E). Null means no active offer (~55% of customers). |
| `contract` | The billing contract type. Month-to-Month customers churn at 45.6% vs only 2.6% for Two Year contracts — the strongest contractual churn signal. |
| `paperless_billing` | Whether the customer receives bills electronically rather than by post. |
| `payment_method` | How the customer pays their bill. Mailed Check customers have the highest churn rate (36.8%). |

---

## Phone Services

| Field | Business Meaning |
|---|---|
| `phone_service` | Whether the customer has an active phone line. |
| `avg_monthly_long_distance_charges` | Average monthly spend on long-distance calls, in USD. Zero when the customer has no phone service. |
| `multiple_lines` | Whether the customer has more than one phone line on the account. |

---

## Internet Services

| Field | Business Meaning |
|---|---|
| `internet_service` | Whether the customer has any internet subscription. |
| `internet_type` | The technology used to deliver internet service. Fiber Optic customers churn at 40.6% — the highest of any internet type. Null when `internet_service = No`. |
| `avg_monthly_gb_download` | Average gigabytes downloaded per month. Zero when no internet service. |
| `premium_tech_support` | Whether the customer pays for the premium technical support add-on. |
| `streaming_tv` | Whether the customer uses the streaming TV service. |
| `streaming_movies` | Whether the customer uses the streaming movies service. |
| `streaming_music` | Whether the customer uses the streaming music service. |
| `unlimited_data` | Whether the customer is on an unlimited data plan. |

---

## Billing & Revenue

| Field | Business Meaning |
|---|---|
| `monthly_charge` | The customer's current monthly bill in USD. Churned customers pay on average \$137.53/month vs \$120.82 for customers who stayed. |
| `total_charges` | Total amount billed to the customer since joining. Null for ~5% of customers (new joiners with no completed billing cycle). |
| `total_refunds` | Total amount refunded to the customer since joining. |
| `total_extra_data_charges` | Total charges for data usage beyond the plan limit since joining. |
| `total_long_distance_charges` | Total long-distance call charges since joining. |
| `total_revenue` | Net revenue from this customer: `total_charges − total_refunds + total_extra_data_charges + total_long_distance_charges`. |
| `cltv` | Customer Lifetime Value — the source's estimate of the customer's total future value. Range: 2,003–6,500. |

---

## Usage — Voice & Messaging

| Field | Business Meaning |
|---|---|
| `calls_1m` | Number of voice calls made in the last month. |
| `calls_3m` | Number of voice calls made in the last 3 months (includes the last month). |
| `sms_1m` | Number of SMS messages sent in the last month. |
| `sms_3m` | Number of SMS messages sent in the last 3 months (includes the last month). |

> ⚠️ **Scoring note:** `calls_*` and `sms_*` are almost perfectly correlated with the churn label (correlation ≈ −0.93). They must not be used for risk scoring without an ablation test, as required by `data_contract.md §7`.

---

## Usage — Data & Purchases

| Field | Business Meaning |
|---|---|
| `data_usage_gb_1m` | Gigabytes of mobile/broadband data consumed in the last month. |
| `data_usage_gb_3m` | Gigabytes of data consumed in the last 3 months. Always equals `3 × avg_monthly_gb_download`. |
| `purchases_1m` | Number of add-on or top-up purchases made in the last month. |
| `purchases_3m` | Number of add-on or top-up purchases in the last 3 months (includes the last month). |

---

## Customer Service

| Field | Business Meaning |
|---|---|
| `customer_service_calls_1m` | Number of times the customer contacted customer support in the last month. Churned customers average 2.54 calls/month vs 0.95 for customers who stayed — a strong churn signal. |
| `customer_service_calls_3m` | Customer support contacts in the last 3 months (includes the last month). The highest non-locked correlation with churn (0.69). |

---

## Churn & Status (🔒 Evaluation-Only)

> These fields describe what actually happened to the customer. They are stored and shown on dashboards but **must never be used to compute risk scores or derived fields**.

| Field | Business Meaning |
|---|---|
| `customer_status` 🔒 | The customer's outcome at the end of Q3 2025: `Stayed`, `Churned`, or `Joined` (new this quarter). |
| `churn_label` 🔒 | Binary churn flag: `Yes` = churned, `No` = did not churn. Always matches `customer_status = Churned`. |
| `churn_score` 🔒 | The dataset's own churn probability score (0–100). Higher = more likely to churn. **Cannot be used for risk scoring** (it is itself the label proxy). |
| `churn_category` 🔒 | Broad category of the reason the customer left: `Competitor`, `Attitude`, `Dissatisfaction`, `Price`, or `Other`. Null for customers who did not churn. |
| `churn_reason` 🔒 | Specific reason the customer gave for leaving (one of 20 values). Top reasons: *Competitor had better devices* (8,818), *Competitor made better offer* (8,618). Null for customers who did not churn. |
| `churn_date` 🔒 | The date the customer churned, between 2025-07-01 and 2025-09-30. Null for customers who did not churn. |
| `satisfaction_score` 🔒 | Customer satisfaction rating 1–5. Scores 1–2 correspond to 100% churn rate; scores 4–5 to 0%. **Cannot be used for risk scoring** (direct label leakage). |

---

## Producer-Added Field

| Field | Business Meaning |
|---|---|
| `event_timestamp` | UTC timestamp of when the producer published this record to Kafka. This is an **ingestion time**, not the time of any customer action. Added by the producer; not present in the original CSV. |

---

## Fields NOT Sent to Kafka

The following 6 columns exist in the CSV but are excluded by the producer (`data_contract.md §6.1`):

| Column | Reason excluded |
|---|---|
| `country` | Constant value: `United States` for all 200,000 rows |
| `state` | Constant value: `California` for all rows |
| `quarter` | Constant value: `Q3` for all rows |
| `churn_value` | Exact duplicate of `churn_label` encoded as `1`/`0` |
| `zip_code` | Not used by scoring or dashboard (can be added later) |
| `population` | Not used by scoring or dashboard |
