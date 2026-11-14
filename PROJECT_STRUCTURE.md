# 📁 Project Repository Structure & Ownership Guide

This document defines the folder layout, module owners, branch rules and peer reviews for the **Real-Time Customer Churn Analytics** repository. Every team member places their deliverables in the paths below to avoid merge conflicts.

**Legend:** *(nice-to-have)* = build only after all must-have items work. Every file has **one owner** (accountable) and, where needed, one contributor.

**Database:** the team uses **MySQL**. See the "Database: MySQL" section of the main `README.md` for the settings that depend on it.

---

## 🌳 Complete Directory Tree

```text
churn-project/
│
├── README.md                           # Main project overview & quickstart (Owner: Member 1)
├── PROJECT_STRUCTURE.md                # This guide (Owner: Member 1)
├── docker-compose.yml                  # Unified stack: Kafka, NiFi, Spark, MySQL, Grafana (Owner: Member 5)
├── .env.example                        # Template for ports, DB credentials, logins (Owner: Member 5)
├── .gitignore                          # Ignores .env, __pycache__, checkpoints, local volumes (Owner: Member 1)
│
├── reference/
│   └── plan_catalog.csv                # MASTER plan catalog: plan_id, name, speed, price (Owner: Member 1)
│
├── docs/                               # 📚 Documentation & Contracts
│   ├── architecture_diagram.png        # Pipeline diagram, linked from README (Owner: Member 1)
│   ├── environment.md                  # How to start and use the Docker environment (Owner: Member 5)
│   ├── data_contract.md                # Schema v1.3, validation & enrichment rules (Owner: Member 1)
│   ├── data_dictionary.md              # Business meaning of each field ONLY; rules live in the contract (Owner: Member 6)
│   ├── nifi_rules.md                   # NiFi processors, cleaning limits & routing logic (Owner: Member 2)
│   ├── test_report.md                  # QA test cases & results (Owner: Member 3, Contributor: Member 6)
│   └── final_presentation.pdf          # Final slide deck (Owner: Member 1)
│
├── generator/                          # ⚙️ Data Simulation & Kafka Producer (Owner: Member 6)
│   ├── README.md                       # How to run the producer in normal or chaos mode
│   ├── requirements.txt                # Python dependencies (confluent-kafka / kafka-python, faker)
│   ├── config.py                       # Copy of plan catalog, city lists, churn correlation weights
│   ├── customer_state.py               # Stateful 180-day plan history, backdating, churn lifecycle
│   ├── fault_injector.py               # `--inject-errors`: errors cleaning cannot repair, one per rule (simple version first)
│   ├── producer.py                     # CLI entry point publishing JSON snapshots to `churn.raw`
│   └── data/
│       ├── sample_valid_records.json   # 100 valid records matching the contract
│       ├── golden_test_data.json       # Small deterministic dataset: includes duplicate snapshots of the same customer and invalid records
│       └── golden_expected.json        # Known correct results for the golden data (state table rows, aggregates, DLQ counts)
│
├── nifi/                               # 🔄 Apache NiFi ETL Flow (Owner: Member 2)
│   ├── README.md                       # How to import the flow and enable controller services
│   ├── churn_etl_flow.json             # Exported NiFi flow definition
│   └── lookup_tables/
│       └── plan_catalog.csv            # COPY of reference/plan_catalog.csv. Do not edit here
│
├── spark/                              # ⚡ Apache Spark Structured Streaming (Owner: Member 4)
│   ├── README.md                       # How to submit the job and inspect checkpoints
│   ├── requirements.txt                # PySpark & MySQL JDBC driver notes
│   ├── streaming_job.py                # Main driver: reads churn.clean, calls the modules below (Owner: Member 4)
│   ├── schemas.py                      # StructType definitions for `churn.clean` & `churn.dlq` (Owner: Member 4)
│   ├── transformations/
│   │   ├── customer_state_upsert.py    # Dedup in micro-batch + upsert of latest snapshot per customer (Owner: Member 4)
│   │   ├── summary_aggregates.py       # Churn by plan and by age group, written to summary tables (Owner: Member 4)
│   │   ├── interaction_analytics.py    # Churn by complaint type and by plan-change type, CS call windows (Owner: Member 3)
│   │   │                               #   spike indicator and high-risk detection are (nice-to-have)
│   │   └── dlq_sink.py                 # Reads churn.dlq into `dlq_errors` (nice-to-have) (Owner: Member 4)
│   └── utils/
│       └── db_upsert.py                # Reusable `foreachBatch` JDBC upsert (MySQL ON DUPLICATE KEY UPDATE); overwrites a row
│                                       #   only if the new event_timestamp is newer (Owner: Member 3)
│
├── sql/                                # 🗄️ MySQL Database (Owner: Member 3)
│   ├── README.md                       # Connection details and table/view descriptions
│   ├── 01_schema.sql                   # DDL: `customers_current_state`, summary tables, `dlq_errors` (nice-to-have)
│   ├── 02_indexes.sql                  # Indexes on segment columns (`plan_id`, `location`, `churn_status`)
│   ├── 03_views.sql                    # Views: demographic (gender, location, income), behavior comparison
│   └── 04_seed_plans.sql               # COPY of the plan catalog as a reference table. Do not edit here
│
├── dashboard/                          # 📊 Grafana Dashboard (Owner: Member 5, built on Member 3's tables and views)
│   ├── README.md                       # How to open the dashboard and import saved panels
│   ├── dashboard_export.json           # Exported Grafana dashboard
│   ├── queries.sql                     # Exact SQL query behind each panel
│   └── screenshots/                    # Backup screenshots for the final report
│
└── scripts/                            # 🛠️ Automation, DevOps & QA Scripts
    ├── create_topics.sh                # Creates `churn.raw`, `churn.clean`, `churn.dlq` (Owner: Member 5)
    ├── init_db.sh                      # Runs `sql/*.sql` against the running MySQL database (Owner: Member 5)
    ├── submit_spark.sh                 # `spark-submit` with Kafka and MySQL JDBC packages (Owner: Member 4, Contributor: Member 5)
    ├── reset_environment.sh            # Clears topics, Spark checkpoints and database tables (Owner: Member 5)
    └── verify_pipeline.py              # Automated end-to-end test (Owner: Member 3, uses Member 6's golden data)
```

---

## 🧭 Single Sources of Truth

Copies drift apart, so every fact has one master.

| Fact | Master | Copies that must match |
|---|---|---|
| Field names, types, rules | `docs/data_contract.md` | `data_dictionary.md` adds business meaning only and never repeats rules |
| Plan catalog | `reference/plan_catalog.csv` | `generator/config.py`, `nifi/lookup_tables/plan_catalog.csv`, `sql/04_seed_plans.sql` |
| Topic names, partitions, keys | `scripts/create_topics.sh` | `docs/environment.md` |

`scripts/verify_pipeline.py` checks that the plan catalog copies match the master.

---

## 🧪 Test Data and Verification

- **`golden_test_data.json` (Member 6)** is a small dataset where the correct results are known. It must include duplicate snapshots of the same customer (to test that only the newest counts), an out-of-order snapshot, and a few invalid records covering several rules.
- **`golden_expected.json` (Member 6)** holds the correct results: the expected rows of `customers_current_state`, the expected summary-table values and the expected number of records in `churn.dlq`.
- **`verify_pipeline.py` (Member 3)** starts from a reset environment, publishes the golden data, **waits until processing has finished** (for example, until Kafka consumer lag is zero), then compares the database and `churn.dlq` with `golden_expected.json` and prints a pass or fail per check.

---

## 🔗 Spark and Database Interfaces

Member 4 and Member 3 both write Spark code, so they agree on these points first:
- `streaming_job.py` (Member 4) reads `churn.clean`, parses it with `schemas.py`, and calls the modules in `transformations/`.
- Each module in `transformations/` exposes one function that takes the parsed data and the shared database writer. Member 4 and Member 3 fix the function signature before coding.
- `db_upsert.py` (Member 3) is the only code that writes to the database. It uses MySQL's `INSERT ... ON DUPLICATE KEY UPDATE` (see the "Database: MySQL" section of `README.md`).
- Tables written by Spark: `customers_current_state` (state), `churn_by_plan` and `churn_by_age_group` (Member 4), `churn_by_complaint`, `churn_by_plan_change` and `cs_calls_windowed` (Member 3). Member 3 designs all the DDL.

---

## 🔐 Branching & Pull Request Rules (Enforced by Member 1)

1. **Protected `main` branch:** nobody pushes directly to `main`.
2. **One feature branch per module:**
   - `infra/docker-kafka` (**Member 5**)
   - `feat/dashboard` (**Member 5**)
   - `feat/generator-producer` (**Member 6**)
   - `feat/nifi-etl` (**Member 2**)
   - `feat/spark-core` (**Member 4**)
   - `feat/sql-analytics` (**Member 3**)
   - `docs/contract` (**Member 1**)
3. **Definition of Done for any PR:**
   - Code follows `docs/data_contract.md` (v1.3) field names, types and rules exactly.
   - The folder `README.md` is updated if any command or configuration changed.
   - Approved by the assigned peer reviewer (below) and then by **Member 1**, who merges.

---

## 🔁 Peer Review Ring

Each person reviews the stage upstream of them, so everyone sees the full flow.

| Reviewer | Reviews | What to check | What the reviewer must produce |
|---|---|---|---|
| **Member 6** | Member 5: environment (`infra/docker-kafka`) | `docker compose up` works from scratch; topics exist with the right names and keys; a producer on the laptop can connect | A short note: how the environment starts and how Kafka topics work |
| **Member 2** | Member 6: generator and golden data | Messages match the contract; churn patterns are realistic; fault injection breaks the intended rules | 5 sample records checked against the 8 validation rules |
| **Member 4** | Member 2: NiFi flow | `churn.clean` has the 6 enrichment fields; invalid records reach `churn.dlq` with the right code; cleaning never repairs values | A short note: what NiFi changes between raw and clean data |
| **Member 3** | Member 4: Spark core | Schemas match the contract; the upsert keeps only the newest snapshot; checkpoints work | One churn rate calculated by hand and compared to the result in the database |
| **Member 5** | Member 3: database and analytics | Tables match the Spark output; the upsert ignores older snapshots; views are correct | A short note: how data gets from the database to the dashboard |
| **Member 4** | Member 5: dashboard | Every panel matches its table or view and its query in `queries.sql` | A list of panels checked against the database |
| **Member 6** and **Member 3** | Member 1: contract, README and final report | Documents match the real behavior of the pipeline | Comments on anything unclear or outdated |

**Review rules:**
1. The reviewer **runs the stage** from its README, not just reads the code.
2. The reviewer asks the author **at least 3 questions**. A question the author cannot answer is a gap to fix.
3. **Member 1** checks every PR for contract compliance before merging.
