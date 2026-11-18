# 📁 Project Repository Structure Guide

This document defines the folder layout, module responsibilities and branch rules for the **Real-Time Customer Churn Analytics** repository. Every deliverable has a fixed path to avoid merge conflicts.

**Legend:** *(nice-to-have)* = build only after all must-have items work. *(undecided)* = the team has not decided whether to build it. *(dataset-dependent)* = keep, change or remove it depending on the dataset the team chooses. Roles and file owners are assigned separately, after the team agrees on this structure.

**Database:** the team uses **MySQL**. See the "Database: MySQL 8" section of the main `README.md` for the settings that depend on it.

**Dataset:** not final yet. The dataset files are **not** committed to Git. The fields in `docs/data_contract.md` (v1.3) stay as they are until the dataset is chosen.

---

## 🌳 Complete Directory Tree

```text
churn-project/
│
├── README.md                           # Main project overview
├── PROJECT_STRUCTURE.md                # This guide
├── docker-compose.yml                  # Unified stack: Kafka, NiFi, Spark, MySQL, Grafana
├── .env.example                        # Template for ports, DB credentials, logins
├── .gitignore                          # Ignores .env, data/raw/, __pycache__, checkpoints, local volumes
├── .gitattributes                      # Line-ending normalization
│
├── data/                               # 📦 Dataset (not committed)
│   ├── README.md                       # Which dataset files are used and where to put them
│   └── raw/                            # GIT-IGNORED: the dataset files
│
├── reference/
│   └── plan_catalog.csv                # MASTER plan catalog: plan_id, name, speed, price (dataset-dependent)
│
├── docs/                               # 📚 Documentation & Contracts
│   ├── architecture_diagram.png        # Pipeline diagram, linked from README
│   ├── environment.md                  # How to start and use the Docker environment
│   ├── data_contract.md                # Schema v1.3, validation & enrichment rules
│   ├── data_dictionary.md              # Business meaning of each field ONLY; rules live in the contract
│   ├── nifi_rules.md                   # NiFi processors, cleaning limits & routing logic
│   ├── scoring_rules.md                # Risk signals, points and thresholds. TO BE AGREED
│   ├── test_report.md                  # QA test cases & results
│   └── final_presentation.pdf          # Final slide deck
│
├── producer/                           # ⚙️ Dataset Replay & Kafka Producer
│   ├── README.md                       # How to run the producer in normal or fault-injection mode
│   ├── requirements.txt                # Python dependencies (confluent-kafka / kafka-python, pandas)
│   ├── config.py                       # Kafka address, topic name, column mapping, replay defaults
│   ├── dataset_loader.py               # Reads the dataset files from data/raw/ and prepares the records in time order
│   ├── fault_injector.py               # `--inject-errors`: errors cleaning cannot repair, one per rule (simple version first)
│   ├── producer.py                     # CLI entry point: replays JSON records to `churn.raw`, earliest first
│   └── data/
│       ├── sample_valid_records.json   # 100 valid records matching the contract
│       ├── golden_test_data.json       # Small deterministic dataset: includes duplicate snapshots of the same customer and invalid records
│       └── golden_expected.json        # Known correct results for the golden data (state table rows, aggregates, DLQ counts)
│
├── nifi/                               # 🔄 Apache NiFi ETL Flow
│   ├── README.md                       # How to import the flow and enable controller services
│   ├── churn_etl_flow.json             # Exported NiFi flow definition
│   └── lookup_tables/
│       └── plan_catalog.csv            # COPY of reference/plan_catalog.csv. Do not edit here (dataset-dependent)
│
├── spark/                              # ⚡ Apache Spark Structured Streaming
│   ├── README.md                       # How to submit the job and inspect checkpoints
│   ├── requirements.txt                # PySpark & MySQL JDBC driver notes
│   ├── streaming_job.py                # Main driver: reads churn.clean from the earliest offset, calls the modules below
│   ├── schemas.py                      # StructType definitions for `churn.clean` & `churn.dlq`
│   ├── transformations/
│   │   ├── customer_state_upsert.py    # Dedup in micro-batch + upsert of latest snapshot per customer
│   │   ├── summary_aggregates.py       # Churn by plan and by age group, written to summary tables (dataset-dependent)
│   │   ├── interaction_analytics.py    # Churn by complaint type and by plan-change type, CS call windows (dataset-dependent)
│   │   │                               #   spike indicator is (nice-to-have)
│   │   ├── risk_scoring.py             # Applies the points from docs/scoring_rules.md to each customer's behavior over time
│   │   └── dlq_sink.py                 # Reads churn.dlq into `dlq_errors` (undecided)
│   └── utils/
│       └── db_upsert.py                # Reusable `foreachBatch` JDBC upsert (MySQL ON DUPLICATE KEY UPDATE); overwrites a row
│                                       #   only if the new event_timestamp is newer
│
├── sql/                                # 🗄️ MySQL Database
│   ├── README.md                       # Connection details and table/view descriptions
│   ├── 01_schema.sql                   # DDL: `customers_current_state`, summary tables, risk flag tables, `dlq_errors` (undecided)
│   ├── 02_indexes.sql                  # Indexes on segment columns (`plan_id`, `location`, `churn_status`, risk level)
│   ├── 03_views.sql                    # Views: demographic (gender, location, income), behavior comparison, customers at risk
│   └── 04_seed_plans.sql               # COPY of the plan catalog as a reference table. Do not edit here (dataset-dependent)
│
├── dashboard/                          # 📊 Grafana Dashboard (built on the tables and views in sql/)
│   ├── README.md                       # How to open the dashboard and import saved panels
│   ├── dashboard_export.json           # Exported Grafana dashboard
│   ├── queries.sql                     # Exact SQL query behind each panel
│   └── screenshots/                    # Backup screenshots for the final report
│
└── scripts/                            # 🛠️ Automation, DevOps & QA Scripts
    ├── requirements.txt                # Python dependencies for the scripts (pymysql)
    ├── create_topics.sh                # Creates `churn.raw`, `churn.clean`, `churn.dlq` (DLQ with no time limit)
    ├── init_db.sh                      # Runs `sql/*.sql` against the running MySQL database
    ├── submit_spark.sh                 # `spark-submit` with Kafka and MySQL JDBC packages
    ├── reset_environment.sh            # Clears topics, Spark checkpoints and database tables
    └── verify_pipeline.py              # Automated end-to-end test (uses the golden data)
```

---

## 🧭 Single Sources of Truth

Copies drift apart, so every fact has one master.

| Fact | Master | Copies that must match |
|---|---|---|
| Field names, types, rules | `docs/data_contract.md` | `data_dictionary.md` adds business meaning only and never repeats rules |
| Plan catalog (dataset-dependent) | `reference/plan_catalog.csv` | `nifi/lookup_tables/plan_catalog.csv`, `sql/04_seed_plans.sql` |
| Risk signals, points and thresholds | `docs/scoring_rules.md` | `spark/transformations/risk_scoring.py` must match it |
| Topic names, partitions, keys, retention | `scripts/create_topics.sh` | `docs/environment.md` |
| Dataset files | The dataset source | Not stored in Git. Each person puts them in `data/raw/` |

`scripts/verify_pipeline.py` checks that the plan catalog copies match the master.

---

## 🧪 Test Data and Verification

- **`golden_test_data.json`** is a small dataset where the correct results are known. It must include duplicate snapshots of the same customer (to test that only the newest counts), an out-of-order snapshot, and a few invalid records covering several rules. Its contents follow the contract fields, so they are revisited when the dataset is chosen.
- **`golden_expected.json`** holds the correct results: the expected rows of `customers_current_state`, the expected summary-table values, the expected number of records in `churn.dlq`, and (once the points are agreed) the expected risk flags.
- **`verify_pipeline.py`** starts from a reset environment, publishes the golden data, **waits until processing has finished** (for example, until Kafka consumer lag is zero), then compares the database and `churn.dlq` with `golden_expected.json` and prints a pass or fail per check.

---

## 🔗 Spark and Database Interfaces

Everyone who writes Spark or SQL code agrees on these points first:
- `streaming_job.py` reads `churn.clean`, parses it with `schemas.py`, and calls the modules in `transformations/`.
- Each module in `transformations/` exposes one function that takes the parsed data and the shared database writer. The function signatures are fixed before coding.
- `db_upsert.py` is the only code that writes to the database. It uses MySQL's `INSERT ... ON DUPLICATE KEY UPDATE` (see the "Database: MySQL 8" section of `README.md`).
- Tables written by Spark: `customers_current_state` (state), `churn_by_plan` and `churn_by_age_group`, `churn_by_complaint`, `churn_by_plan_change` and `cs_calls_windowed`, plus the risk flag tables written by `risk_scoring.py`. The risk flag tables and fields are defined once the points are agreed. The SQL owner designs all the DDL.

---

## 🔐 Branching & Pull Request Rules (Enforced by the Team Leader)

1. **Protected `main` branch:** nobody pushes directly to `main`.
2. **One feature branch per module:**
   - `infra/docker-kafka`
   - `feat/dashboard`
   - `feat/producer`
   - `feat/nifi-etl`
   - `feat/spark-core`
   - `feat/spark-scoring`
   - `feat/sql-analytics`
   - `docs/contract`
3. **Definition of Done for any PR:**
   - Code follows `docs/data_contract.md` (v1.3) field names, types and rules exactly.
   - The folder `README.md` is updated if any command or configuration changed.
   - Approved by a teammate who **ran the stage** from its README, and then by the **team leader**, who checks contract compliance and merges.
