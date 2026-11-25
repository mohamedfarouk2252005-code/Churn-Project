# Producer — Dataset Replay

Replays `churn_raw.csv` (200,000 rows, 52 fields + `event_timestamp`) into the Kafka topic `churn.raw`, one JSON message per row.

Implements **data_contract.md v2.3, sections 6.1 – 6.5**.

---

## Setup

```bash
# From the project root
pip install -r producer/requirements.txt

# Place the dataset (git-ignored):
# data/raw/churn_raw.csv
```

---

## Run

```bash
# From the project root:

# Normal replay — 10 records/sec (≈ 5.6 hours for 200k rows)
python producer/producer.py

# Faster replay — 100 records/sec (≈ 33 minutes)
python producer/producer.py --rate 100

# With fault injection (tests all 6 NiFi rules)
python producer/producer.py --rate 10 --inject-errors
```

---

## What it does

| Step | Detail |
|---|---|
| Load CSV | Reads `data/raw/churn_raw.csv` via `dataset_loader.py` **in file order** (§6.3) |
| Drop 6 columns | `country`, `state`, `quarter`, `zip_code`, `population`, `churn_value` are excluded (§6.1) |
| Type serialization | Integer fields → JSON `int`, float fields → JSON `float`, string fields → JSON `string`. Empty nullable cells → `null`. Empty required cells → `""` for NiFi to reject (§6.2) |
| `event_timestamp` | Real UTC wall-clock time added to every message (§6.3) |
| Kafka key | `customer_id` bytes when non-empty; `null` Kafka key when `customer_id` is empty (§6.4) |
| Fault injection | `--inject-errors` interleaves records from `fault_injector.py`, one per NiFi rule (§6.5) |

---

## Files

| File | Purpose |
|---|---|
| `producer.py` | CLI entry point |
| `config.py` | Kafka bootstrap, topic, field lists |
| `dataset_loader.py` | CSV → raw string dicts, validates headers |
| `fault_injector.py` | 8 deliberately broken records (one per NiFi rule) |
| `data/sample_valid_records.json` | 100 valid records for smoke tests |
| `data/golden_test_data.json` | Deterministic test batch (duplicates, invalids, risk records) |
| `data/golden_expected.json` | Expected pipeline output for the golden batch |

---

## Kafka topics

| Topic | Listener (from laptop) |
|---|---|
| `churn.raw` | `localhost:9094` |

Topics are created by `scripts/create_topics.sh`. Run that first.
