# 📡 Real-Time Customer Churn Analytics

## 📝 Description
**Customer churn** happens when a customer cancels their subscription and leaves the company. For a telecom provider, losing customers is expensive, because winning a new customer costs far more than keeping an existing one. The goal of this project is to **replay customer data as a live stream, process it immediately, analyze each customer's behavior over time, and flag the customers who are at risk** before they go.

The project is a real-time big data pipeline built with **Apache Kafka, Apache NiFi, Apache Spark Structured Streaming, a MySQL 8 database and a Grafana dashboard**. A Python producer reads a dataset and sends it to Kafka in time order, earliest first, so the dashboard updates continuously as the data arrives.

![Architecture diagram](docs/architecture_diagram.png)

```text
Dataset ──► Python producer ──► Kafka (churn.raw) ──► NiFi (ETL) ──┬──► Kafka (churn.clean) ──► Spark Streaming ──► MySQL ──► Grafana
            (replay, earliest first)                               │                          (analysis + risk points)
                                                                   └──► Kafka (churn.dlq)  (invalid records + error_reason, kept for later)
```

---

## 📊 Data Description
The data comes from a dataset that the producer replays. **The dataset is not final yet.** The fields below are the ones defined in [`docs/data_contract.md`](docs/data_contract.md) (v1.3), which is the single source of truth. They stay as they are until the team chooses the dataset, and then the contract and this section are revisited together.

Each Kafka message is a **snapshot of one customer at a point in time**, in JSON format. The database keeps the latest snapshot per `customer_id`, so no customer is ever counted twice.

| Group | Fields |
|---|---|
| **Demographics** | `customer_id`, `age`, `gender`, `location`, `income_level` |
| **Service usage (30d)** | `tenure_months`, `calls_count`, `messages_count`, `data_usage_mb` |
| **Plan and billing** | `plan_id`, `plan_speed_mbps`, `monthly_charges`, `payment_method` |
| **Plan changes (6m)** | `last_plan_changed_from`, `last_plan_changed_to`, `last_plan_change_type`, `plan_changes_6m` (rolling 180-day window) |
| **Customer service** | `cs_calls_last_7d`, `cs_calls_last_30d`, `last_complaint_type` |
| **Purchases (30d)** | `purchase_frequency`, `days_since_last_purchase` |
| **Churn & metadata** | `churn_status`, `churn_date`, `event_timestamp` (real UTC time) |
| **NiFi enrichments** | `age_group`, `tenure_group`, `purchase_segment`, `downgrade_flag`, `frequent_plan_changer_flag`, `high_complaint_flag` |

### Plan Catalog
The speeds are in Mbps. The prices are simulation values in EGP, and the monthly charge is always determined by the plan. The master file is `reference/plan_catalog.csv`.

| `plan_id` | Name | Speed (Mbps) | Monthly charge (EGP) |
|---|---|---|---|
| 1 | Starter | 30 | 200 |
| 2 | Basic | 50 | 300 |
| 3 | Plus | 70 | 400 |
| 4 | Pro | 100 | 550 |
| 5 | Ultra | 150 | 750 |

### Complaint Types
`slow_internet`, `frequent_disconnection`, `billing_issue`, `technical_fault`, `plan_inquiry`, `other`, `none`

---

## ⚖️ Project Rules
- **Data contract:** All stages use the same JSON schema (`docs/data_contract.md` v1.3), topic names and message key (`customer_id`). Any change must be approved by the team leader and announced to everyone the same day.
- **Replay order:** The producer sends the data in time order, earliest first. The dataset files are not committed to Git (they stay in `data/raw/`).
- **Producer does not clean:** The producer only sends the records. Dirty data must reach NiFi, so that cleaning and validation do the work.
- **Plan and price consistency:** `monthly_charges` and `plan_speed_mbps` must match `plan_id`.
- **Plan changes (6-month window):** Moving to a higher plan ID is an `upgrade`, to a lower ID a `downgrade`, and no change within the last 180 days is `none`.
- **Customer service calls:** `cs_calls_last_7d` can never be greater than `cs_calls_last_30d`. If `cs_calls_last_30d = 0`, `last_complaint_type` must be `none`.
- **Cleaning fixes formatting only:** NiFi trims spaces and fixes letter case and date formats, but never repairs values, so real errors reach the dead-letter queue.
- **Invalid records:** Records that break a rule are never dropped silently. They get an `error_reason` code (the first failed rule), a `failed_at_timestamp` and a `raw_payload` when the message could not be parsed, and go to `churn.dlq`.
- **The DLQ is kept:** `churn.dlq` has no time limit and nothing consumes it for now. The records stay there to be read later, and the team decides what to do with them.
- **Churn lifecycle:** A churned customer sends one final snapshot with `churn_status = "Yes"`, then no more snapshots.
- **Newest snapshot wins:** The database overwrites a customer row only if the incoming `event_timestamp` is newer than the stored one.
- **Summaries come from the current state:** Summary tables are recomputed from `customers_current_state`, never aggregated directly from the stream, so a customer whose row is overwritten is never double-counted.
- **Flags use points:** Spark flags customers at risk with a points system. The signals and points are written in one place, `docs/scoring_rules.md`, and are **to be agreed** by the team.
- **Teamwork:** Each module is developed on its own Git branch and submitted through a pull request. Every stage is reviewed by a teammate, and by the team leader for contract compliance, before merging to `main`.

---

## 🧱 Pipeline Components

### 1. Dataset and Kafka Producer (`producer/`)
A Python producer reads the dataset from `data/raw/` and replays it to `churn.raw` in time order, **earliest first**, using `customer_id` as the message key. A `--inject-errors` option sends records that break the validation rules, to test the dead-letter queue.

### 2. Apache Kafka
Kafka decouples the stages and keeps message order per customer through `customer_id` partitioning.

| Topic | Purpose | Retention |
|---|---|---|
| `churn.raw` | Raw customer records from the Python producer | 7 days |
| `churn.clean` | Validated, cleaned and enriched records from NiFi | 7 days |
| `churn.dlq` | Invalid records rejected by NiFi (with `error_reason`) | **No time limit** |

### 3. Apache NiFi (ETL) (`nifi/`)
NiFi consumes `churn.raw` and performs, in this order:
- **Cleaning:** formatting only (trim spaces, fix letter case, standardize gender words and date formats, empty strings to `null`).
- **Validation:** 8 rules, evaluated in order (parsing and types, plan/price match, call windows, complaint consistency, 6-month plan-change consistency, purchase rules, churn date, allowed values).
- **Enrichment:** adds `age_group`, `tenure_group`, `purchase_segment`, `downgrade_flag`, `frequent_plan_changer_flag` and `high_complaint_flag`.
- **Routing:** valid records go to `churn.clean`, invalid records go to `churn.dlq` and stay there.

### 4. Apache Spark Structured Streaming (`spark/`)
Spark reads `churn.clean` from the earliest offset, parses it with an explicit schema, uses checkpointing for fault tolerance and recovery, and writes to MySQL through `foreachBatch`. The customer state lives in MySQL, and the job uses no event-time windows, so it does not need watermarks.

Each micro-batch follows the same steps (implemented once in `spark/utils/db_upsert.py`):
1. **Dedupe** the micro-batch: keep the newest row per `customer_id`.
2. **Stage:** write the rows to a staging table over JDBC (Spark cannot upsert directly).
3. **Upsert:** run one `INSERT ... SELECT ... ON DUPLICATE KEY UPDATE` from the staging table into `customers_current_state`, overwriting a row only if the new `event_timestamp` is newer.
4. **Recompute summaries** from `customers_current_state`: churn by plan and by age group, churn by complaint type and by plan-change type, and customer service call windows (`cs_calls_windowed`).
5. **Flag customers at risk:** apply the points from `docs/scoring_rules.md` to each customer's behavior over time (for example a drop in usage, a rise in customer service calls) and store the score, the risk level and the reasons. The exact tables and fields are added to the contract once the points are agreed.

*(nice-to-have)* Spike indicator. *(undecided)* Loading `churn.dlq` into a `dlq_errors` table.

### 5. MySQL Database and Dashboard (`sql/` and `dashboard/`)
- **MySQL:** stores `customers_current_state`, the staging table, the summary tables and analytical views for the remaining demographic and behavior breakdowns.
- **Dashboard (Grafana):** connects to the tables and views through the built-in MySQL data source and displays live KPIs, churn by segment, call trends and the customers flagged at risk.

---

## 🧠 Analytics Performed

1. **Demographic Analysis:** Churn rate by `age_group` (Spark table), `gender`, `location` and `income_level` (views).
2. **Behavior Analysis:** Churned vs. active customers compared by usage (`calls_count`, `messages_count`, `data_usage_mb`), `monthly_charges`, `payment_method`, `purchase_segment` and `days_since_last_purchase` (views).
3. **Plan & Migration Analysis:** Churn rate by `plan_id` (Spark table), `last_plan_change_type` and `plan_changes_6m` (Spark table).
4. **Customer Interaction Analysis:**
   - Churn rate by `last_complaint_type`
   - Customer service call trends over time windows (`cs_calls_last_7d` vs. `cs_calls_last_30d`, table `cs_calls_windowed`)
   - *(nice-to-have)* Call-spike ratio (`cs_calls_last_7d / cs_calls_last_30d`) as an early warning signal
5. **Risk Flagging (points):** each customer gets points for risk signals, for example a large drop in usage, rising customer service calls, billing behavior and plan changes. The total gives a risk level and the list of reasons. The signals and points are **to be agreed** (`docs/scoring_rules.md`).

---

## 🗄️ Database: MySQL 8

The team uses **MySQL 8.0**, pinned to an exact image tag in `docker-compose.yml` (never `latest`). Row aliases in upserts need 8.0.19 or later.

| Item | Value |
|---|---|
| Docker service | `db`, image `mysql:8.0`, health check with `mysqladmin ping` |
| Port | `3306` inside Docker. On a laptop that already runs MySQL, map `3307:3306` through `DB_HOST_PORT` in `.env` |
| `.env` variables | `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_ROOT_PASSWORD` (the MySQL image requires it), `DB_HOST_PORT` |
| JDBC URL (Spark) | `jdbc:mysql://db:3306/<DB_NAME>?connectionTimeZone=UTC&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true&useSSL=false` (the last two options are for the local development setup only) |
| JDBC driver (Spark) | `com.mysql:mysql-connector-j` (8.x), passed with `--packages` in `submit_spark.sh`. The old `mysql:mysql-connector-java` coordinate is outdated |
| Grafana data source | Built-in MySQL, host `db:3306` |
| Python on the laptop | `pymysql` (listed in `scripts/requirements.txt`) |
| Column types | Timestamps as `DATETIME(3)` in UTC (not `TIMESTAMP`), booleans as `TINYINT(1)`, `churn_date` as `DATE NULL`, `raw_payload` as `TEXT` |

### How the "newest snapshot wins" upsert works
Spark can only append or overwrite over JDBC, so `db_upsert.py` writes each deduplicated micro-batch to a staging table and then runs `INSERT ... SELECT ... ON DUPLICATE KEY UPDATE` into `customers_current_state`.

MySQL applies the assignments of `ON DUPLICATE KEY UPDATE` **from left to right**. So every column keeps its old value unless the incoming `event_timestamp` is newer than the stored one, and `event_timestamp` is assigned **last**. If it were assigned first, every later column would compare against the already updated value and an older snapshot could win.

```sql
INSERT INTO customers_current_state (customer_id, age, ..., event_timestamp)
SELECT s.customer_id, s.age, ..., s.event_timestamp FROM stg_customers s
ON DUPLICATE KEY UPDATE
  age = IF(s.event_timestamp > customers_current_state.event_timestamp, s.age, age),
  -- ...same pattern for every other column...
  event_timestamp = IF(s.event_timestamp > customers_current_state.event_timestamp,
                       s.event_timestamp, event_timestamp);
```

The golden test data contains an out-of-order snapshot that checks exactly this case. The full statement is written by whoever owns the SQL schema; the schema is in `sql/01_schema.sql`.

---

## 🔓 Open Decisions
- **Which dataset** the producer replays. The fields, validation rules and samples in the data contract are revisited after this decision.
- **The scoring points:** the signals, the points for each, and the thresholds for the risk levels (`docs/scoring_rules.md`).
- **The dead-letter queue:** what to do with the records kept in `churn.dlq` (for example a `dlq_errors` table, a dashboard panel, or a replay after fixing the source).
