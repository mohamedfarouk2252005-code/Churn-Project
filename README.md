# 📡 Real-Time Customer Churn Analytics

## 📝 Description
**Customer churn** happens when a customer cancels their subscription and leaves the company. For a telecom provider, losing customers is expensive, because winning a new customer costs far more than keeping an existing one. The goal of this project is to **collect customer data as it happens, process it immediately, and analyze it** to understand who is leaving, why they are leaving, and which customers are at risk before they go.

The project is a real-time big data pipeline built with **Apache Kafka, Apache NiFi, Apache Spark Structured Streaming, a MySQL 8 database and a Grafana dashboard**. Instead of waiting for a monthly report, the system updates continuously so the business can react while there is still time to retain the customer.

![Architecture diagram](docs/architecture_diagram.png)

```text
Data Source ──► Kafka (churn.raw) ──► NiFi (ETL) ──┬──► Kafka (churn.clean) ──► Spark Streaming ──► MySQL ──► Grafana
                                                   └──► Kafka (churn.dlq)  (invalid records + error_reason)
```

---

## 📊 Data Description
Each Kafka message is a **snapshot of one customer at a point in time**, in JSON format. The database keeps the latest snapshot per `customer_id`, so no customer is ever counted twice. The full field list, allowed values and rules are in [`docs/data_contract.md`](docs/data_contract.md) (v1.3), which is the single source of truth.

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
- **Plan and price consistency:** `monthly_charges` and `plan_speed_mbps` must match `plan_id`.
- **Plan changes (6-month window):** Moving to a higher plan ID is an `upgrade`, to a lower ID a `downgrade`, and no change within the last 180 days is `none`.
- **Customer service calls:** `cs_calls_last_7d` can never be greater than `cs_calls_last_30d`. If `cs_calls_last_30d = 0`, `last_complaint_type` must be `none`.
- **Cleaning fixes formatting only:** NiFi trims spaces and fixes letter case and date formats, but never repairs values, so real errors reach the dead-letter queue.
- **Invalid records:** Records that break a rule are never dropped silently. They get an `error_reason` code (the first failed rule), a `failed_at_timestamp` and a `raw_payload` when the message could not be parsed, and go to `churn.dlq`.
- **Churn lifecycle:** A churned customer sends one final snapshot with `churn_status = "Yes"`, then no more snapshots.
- **Newest snapshot wins:** The database overwrites a customer row only if the incoming `event_timestamp` is newer than the stored one.
- **Summaries come from the current state:** Summary tables are recomputed from `customers_current_state`, never aggregated directly from the stream, so a customer whose row is overwritten is never double-counted.
- **Teamwork:** Each member works on their own Git branch and submits through a pull request. Every stage is reviewed by a peer, and by Member 1 for contract compliance, before merging to `main`.

---

## 🧱 Pipeline Components

### 1. Data Source and Kafka Producer (`generator/`)
A stateful Python generator simulates customers with **realistic churn patterns** (downgraders churn more than upgraders, `slow_internet` complaints on fast plans raise churn, spikes in 7-day customer service calls raise churn sharply, and churners buy less often). Timestamps use real UTC time, and plan-change history is backdated when a customer is created. A `--inject-errors` option sends records that break the validation rules, to test the dead-letter queue.

### 2. Apache Kafka
Kafka decouples the stages and keeps message order per customer through `customer_id` partitioning.

| Topic | Purpose |
|---|---|
| `churn.raw` | Raw customer snapshots from the Python producer |
| `churn.clean` | Validated, cleaned and enriched records from NiFi |
| `churn.dlq` | Invalid records rejected by NiFi (with `error_reason`) |

### 3. Apache NiFi (ETL) (`nifi/`)
NiFi consumes `churn.raw` and performs, in this order:
- **Cleaning:** formatting only (trim spaces, fix letter case, standardize gender words and date formats, empty strings to `null`).
- **Validation:** 8 rules, evaluated in order (parsing and types, plan/price match, call windows, complaint consistency, 6-month plan-change consistency, purchase rules, churn date, allowed values).
- **Enrichment:** adds `age_group`, `tenure_group`, `purchase_segment`, `downgrade_flag`, `frequent_plan_changer_flag` and `high_complaint_flag`.
- **Routing:** valid records go to `churn.clean`, invalid records go to `churn.dlq`.

### 4. Apache Spark Structured Streaming (`spark/`)
Spark reads `churn.clean` in real time, parses it with an explicit schema, uses checkpointing for fault tolerance and recovery, and writes to MySQL through `foreachBatch`. The customer state lives in MySQL, and the job uses no event-time windows, so it does not need watermarks.

Each micro-batch follows the same steps (implemented once in `spark/utils/db_upsert.py`):
1. **Dedupe** the micro-batch: keep the newest row per `customer_id`.
2. **Stage:** write the rows to a staging table over JDBC (Spark cannot upsert directly).
3. **Upsert:** run one `INSERT ... SELECT ... ON DUPLICATE KEY UPDATE` from the staging table into `customers_current_state`, overwriting a row only if the new `event_timestamp` is newer.
4. **Recompute summaries** from `customers_current_state`: churn by plan and by age group, churn by complaint type and by plan-change type, and customer service call windows (`cs_calls_windowed`).

*(nice-to-have)* Spike indicator, high-risk customer detection, and loading `churn.dlq` into a `dlq_errors` table.

### 5. MySQL Database and Dashboard (`sql/` and `dashboard/`)
- **MySQL:** stores `customers_current_state`, the staging table, the summary tables and analytical views for the remaining demographic and behavior breakdowns.
- **Dashboard (Grafana):** connects to the tables and views through the built-in MySQL data source and displays live KPIs, churn by segment, call trends and customers at risk.

---

## 🧠 Analytics Performed

1. **Demographic Analysis:** Churn rate by `age_group` (Spark table), `gender`, `location` and `income_level` (views).
2. **Behavior Analysis:** Churned vs. active customers compared by usage (`calls_count`, `messages_count`, `data_usage_mb`), `monthly_charges`, `payment_method`, `purchase_segment` and `days_since_last_purchase` (views).
3. **Plan & Migration Analysis:** Churn rate by `plan_id` (Spark table), `last_plan_change_type` and `plan_changes_6m` (Spark table).
4. **Customer Interaction Analysis:**
   - Churn rate by `last_complaint_type`
   - Customer service call trends over time windows (`cs_calls_last_7d` vs. `cs_calls_last_30d`, table `cs_calls_windowed`)
   - *(nice-to-have)* Call-spike ratio (`cs_calls_last_7d / cs_calls_last_30d`) as an early warning signal
   - *(nice-to-have)* Combined high-risk identification (for example `downgrade_flag = true` with `high_complaint_flag = true` and `slow_internet`)

---

## 👥 Team & Work Distribution

| Member | Role | Core Responsibilities & Deliverables |
|---|---|---|
| **Member 1 (Leader)** | **Architecture, Data Contract & Integration** | • Own `docs/data_contract.md`, `reference/plan_catalog.csv`, `README.md`, `PROJECT_STRUCTURE.md` and the architecture diagram<br>• Manage the Git repo, branch protection, PR reviews and task tracking<br>• Lead end-to-end integration testing<br>• Write the final documentation and presentation |
| **Member 5** | **Infrastructure, Kafka & Dashboard** | • Build `docker-compose.yml` (Kafka KRaft, NiFi, Spark master and worker, MySQL 8 with health check, Kafka UI, Grafana) and `docs/environment.md`<br>• Write `create_topics.sh`, `init_db.sh` and `reset_environment.sh`<br>• Set up persistent volumes, health checks and Kafka lag monitoring<br>• Build the Grafana dashboard (`dashboard/`) on Member 3's tables and views |
| **Member 6** | **Simulation Engine & Test Data** | • Build the stateful generator and Kafka producer with backdated 180-day plan history and the churn lifecycle<br>• Add the `--inject-errors` option for the dead-letter queue test<br>• Maintain `docs/data_dictionary.md`<br>• Create `golden_test_data.json` (with an out-of-order snapshot) and `golden_expected.json` |
| **Member 2** | **NiFi ETL & Data Quality** | • Build the NiFi flow: consume → clean → validate → enrich → publish (`churn.clean` and `churn.dlq`)<br>• Implement all 8 validation rules and 6 enrichment fields from the contract<br>• Attach `error_reason`, `failed_at_timestamp` and `raw_payload` to DLQ messages<br>• Export the flow and write `docs/nifi_rules.md` |
| **Member 4** | **Spark Streaming Core** | • Build `spark/streaming_job.py` (reads `churn.clean`, schemas, checkpoints)<br>• Build the customer state upsert and the summary tables for churn by plan and by age group<br>• Write `scripts/submit_spark.sh` (Kafka connector and MySQL Connector/J packages)<br>• *(nice-to-have)* `dlq_sink.py` |
| **Member 3** | **SQL, Interaction Analytics & Testing** | • Design the MySQL schema (including the staging table), indexes and views (`sql/`)<br>• Build the shared upsert utility `spark/utils/db_upsert.py`<br>• Build the interaction analytics (complaint type, plan-change type, call windows) in Spark<br>• Write `scripts/verify_pipeline.py`, run the golden test and write `docs/test_report.md` |

Peer reviews follow the ring in [`PROJECT_STRUCTURE.md`](./PROJECT_STRUCTURE.md), which also has the complete file-by-file tree and ownership.

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

The golden test data contains an out-of-order snapshot that checks exactly this case. Member 3 writes the full statement; the schema is in `sql/01_schema.sql`.

---

## 🚀 How to Run
1. Clone the repository and copy `.env.example` to `.env`, then set the passwords:
   ```bash
   cp .env.example .env
   ```
2. Start the entire environment in Docker (details in [`docs/environment.md`](docs/environment.md)):
   ```bash
   docker compose up -d
   ```
3. Create the Kafka topics (`churn.raw`, `churn.clean`, `churn.dlq`):
   ```bash
   bash scripts/create_topics.sh
   ```
4. Initialize the database schema and views (if they are not loaded automatically at container start):
   ```bash
   bash scripts/init_db.sh
   ```
5. Import and start the NiFi flow from `nifi/`.
6. Submit the Spark Structured Streaming job:
   ```bash
   bash scripts/submit_spark.sh
   ```
7. Start the Python Kafka producer (add `--inject-errors 0.05` to send 5% invalid records):
   ```bash
   python generator/producer.py --rate 10
   ```
8. Open the Grafana dashboard in your browser to watch the churn metrics update live.

To wipe all data and start clean: `bash scripts/reset_environment.sh`. To run the automated test: `pip install -r scripts/requirements.txt`, then `python scripts/verify_pipeline.py`.
