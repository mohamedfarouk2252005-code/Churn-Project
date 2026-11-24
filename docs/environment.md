# Real-Time Customer Churn Analytics: Environment Setup

**File location in the repo:** `docs/environment.md`
**Approved by:** team leader

This guide explains how to start the whole project environment (Kafka, NiFi, Spark, MySQL, Kafka UI and Grafana) with **one command**, how the services connect to each other, and how to fix the usual problems. The folder layout is in `PROJECT_STRUCTURE.md`.

> **Database:** the team uses **MySQL 8.0**. The image version is pinned in `docker-compose.yml`. The JDBC URL, driver and upsert rules are in the "Database: MySQL 8" section of `README.md`.

> **Dataset:** the dataset is not final yet. Whichever one is chosen, its files go in `data/raw/` (not committed to Git), and nothing in this environment depends on the choice.

---

## 1. Pipeline overview

```
Dataset (data/raw/) ──► Python producer ──► Kafka: churn.raw ──► NiFi (clean, validate, enrich) ──┬──► Kafka: churn.clean ──► Spark Streaming ──► MySQL ──► Grafana
                       (runs on your laptop)                                                      └──► Kafka: churn.dlq  (invalid records, kept)
```

| Stage | Service |
|---|---|
| Dataset replay and producer | Python scripts (run on your own machine) |
| Message broker | Kafka |
| ETL | NiFi |
| Analytics and risk flags | Spark |
| Storage | MySQL |
| Dashboard | Grafana |

---

## 2. Prerequisites

| Requirement | Minimum | Recommended |
|---|---|---|
| RAM | 8 GB | 16 GB |
| Free disk space | 15 GB | 25 GB |
| Docker Desktop (or Docker Engine) with **Compose v2** | Yes | Yes |
| Git | Yes | Yes |
| Python (for the producer and the scripts in `scripts/`) | 3.10 | 3.11 or newer |
| The dataset files in `data/raw/` | Yes | Yes |

**Windows users:** use Docker Desktop with the **WSL2 backend** and run all commands from a WSL2 terminal (Ubuntu), not from CMD.

**Give Docker enough memory:** Docker Desktop → Settings → Resources → set memory to **6 GB or more** (8 GB if you have 16 GB RAM).

```bash
docker --version
docker compose version
python --version
```

---

## 3. Quick start (first time)

```bash
# 1. Get the code
git clone <repo-url>
cd churn-project

# 2. Create your local settings file, then set the passwords (section 8)
cp .env.example .env

# 3. Put the dataset files in data/raw/ (see data/README.md)
ls data/raw

# 4. Start everything (the first run downloads several GB of images)
docker compose up -d

# 5. Create the Kafka topics
bash scripts/create_topics.sh

# 6. Create the database tables and views
bash scripts/init_db.sh

# 7. Check that all is healthy
docker compose ps
```

**Done when:** `docker compose ps` shows every service as `running` (or `healthy`, which `db` must reach), and Kafka UI shows the three topics.

> NiFi is the slowest to start and can take **2 to 4 minutes**. MySQL needs about 30 seconds on the first start while it initializes its data folder. If a page does not open immediately, wait and retry.

### Daily use

| Task | Command |
|---|---|
| Start | `docker compose up -d` |
| Stop (keeps data) | `docker compose stop` |
| Remove containers (keeps data volumes) | `docker compose down` |
| **Wipe everything including data** | `bash scripts/reset_environment.sh` (or `docker compose down -v`) |
| See status | `docker compose ps` |
| Follow logs of one service | `docker compose logs -f kafka` |
| Restart one service | `docker compose restart nifi` |
| Open a MySQL shell | `docker exec -it db mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME"` |
| Submit the Spark job | `bash scripts/submit_spark.sh` |
| Replay the dataset | `python producer/producer.py --rate 10` (add `--inject-errors 0.05` to send 5% invalid records) |

---

## 4. Services, ports and web pages

| Service | Container name | Web page / port on your machine | Notes |
|---|---|---|---|
| Kafka (broker) | `kafka` | `localhost:9094` | KRaft mode, no Zookeeper needed |
| Kafka UI | `kafka-ui` | http://localhost:8090 | Browse topics and messages, see consumer groups and lag |
| NiFi | `nifi` | https://localhost:8443/nifi | The browser warns about the certificate: accept it. Login from `.env` |
| Spark master | `spark-master` | http://localhost:8080 | Jobs connect on port `7077` inside Docker |
| Spark worker | `spark-worker` | http://localhost:8081 | |
| MySQL | `db` | `localhost:3306` (or `localhost:<DB_HOST_PORT>`) | MySQL 8.0. Database name and logins from `.env` |
| Grafana | `grafana` | http://localhost:3000 | Login from `.env` |

If a port is already used on your machine, change the **left** number of the port mapping in `docker-compose.yml` and tell the team. The most common case is a MySQL already installed on your laptop: set `DB_HOST_PORT=3307` in your `.env`, which maps `3307:3306`. Inside Docker the port stays `3306`.

---

## 5. How to connect: the most common source of errors

Containers talk to each other by **container name**. Your own programs (running on your laptop, outside Docker) use **localhost** and a different Kafka port.

| Who is connecting | Kafka address | Database address |
|---|---|---|
| **A program on your laptop** (the producer, `verify_pipeline.py`) | `localhost:9094` | `localhost:<DB_HOST_PORT>` (default `3306`) |
| **A container** (NiFi, Spark, Grafana, Kafka UI) | `kafka:9092` | `db:3306` |

```python
# The producer (runs on the laptop)
bootstrap_servers = "localhost:9094"
```

```
# NiFi processors: Kafka Brokers property
kafka:9092
```

```python
# Spark job (runs in a container)
.option("kafka.bootstrap.servers", "kafka:9092")
.option("startingOffsets", "earliest")
```

JDBC URL for Spark:

```
jdbc:mysql://db:3306/<DB_NAME>?connectionTimeZone=UTC&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true&useSSL=false
```

`allowPublicKeyRetrieval=true` and `useSSL=false` are for this local development setup only. `connectionTimeZone=UTC` keeps timestamps in UTC, and `rewriteBatchedStatements=true` makes the staging writes faster.

> Using `localhost` inside a container points to the container itself, not to Kafka or the database. This is the number one mistake.

**Spark packages:** Spark needs the Kafka connector and the MySQL driver `com.mysql:mysql-connector-j` (8.x). The old `mysql:mysql-connector-java` coordinate is outdated and must not be used. `scripts/submit_spark.sh` passes both with `--packages`, and the Kafka connector version must match the Spark and Scala versions in `docker-compose.yml`.

---

## 6. Database (MySQL)

- **Image:** `mysql:8.0` (pinned tag, never `latest`). Row aliases in upserts need 8.0.19 or later.
- **Health check:** `mysqladmin ping`. Other services that need the database wait until `db` is `healthy`.
- **Init scripts:** the files in `sql/` are mounted into `/docker-entrypoint-initdb.d`. MySQL runs them **only on the first start with an empty volume**. After that, use `bash scripts/init_db.sh`. The SQL files use `CREATE TABLE IF NOT EXISTS` so running the script twice is safe.
- **Time and dates:** timestamps are stored as `DATETIME(3)` in UTC, not `TIMESTAMP`, to avoid time zone conversion surprises. `churn_date` is a `DATE`.
- **Passwords:** the MySQL image needs `DB_ROOT_PASSWORD` in addition to the normal user login (section 8).
- **Tables:** `customers_current_state`, a staging table used by `db_upsert.py`, the summary tables, the risk flag tables and the views. They are described in `sql/README.md`.

---

## 7. Dashboard (Grafana)

The dashboard is built on the tables and views designed in `sql/`.

1. Open http://localhost:3000 and log in with the `.env` values.
2. Add the **data source**: type **MySQL**, host `db:3306`, the database name and the `DB_USER` / `DB_PASSWORD` login from `.env`. Do not use the root login.
3. Import `dashboard/dashboard_export.json`, or build the panels from `dashboard/queries.sql`.
4. Every panel has its exact SQL query in `dashboard/queries.sql`, so a reviewer can check it against the database.

---

## 8. Settings (`.env`)

Copy `.env.example` to `.env` and fill in the values. Each person has their own `.env`, and it is **never** committed to Git.

| Variable | Meaning |
|---|---|
| `DB_NAME` | Database name |
| `DB_USER` / `DB_PASSWORD` | Database login used by Spark, Grafana and the scripts |
| `DB_ROOT_PASSWORD` | MySQL root password (required by the MySQL image) |
| `DB_HOST_PORT` | Port on your laptop that maps to MySQL's `3306` (default `3306`, use `3307` if you already run MySQL) |
| `NIFI_USERNAME` / `NIFI_PASSWORD` | NiFi single-user login (the password must be at least 12 characters) |
| `GRAFANA_USER` / `GRAFANA_PASSWORD` | Grafana admin login |

Image versions are pinned inside `docker-compose.yml`. **Do not change them** without telling the team leader, so everyone runs the same versions.

---

## 9. Kafka topics

Created by `scripts/create_topics.sh`:

| Topic | Purpose | Written by | Read by | Partitions | Retention |
|---|---|---|---|---|---|
| `churn.raw` | Raw customer records | The producer | NiFi | 3 | 7 days |
| `churn.clean` | Validated, cleaned and enriched records | NiFi | Spark | 3 | 7 days |
| `churn.dlq` | Invalid records (dead-letter queue) | NiFi | Nobody for now. Read later by humans, or by `dlq_sink.py` if the team decides to build it | 3 | **No time limit** |

**Message key:** `customer_id`. All messages of one customer go to the same partition, so their order is preserved.

**Message value:** JSON, exactly as defined in `docs/data_contract.md`.

**Why the DLQ has no time limit:** invalid records must stay available until the team decides what to do with them. The volume is small, so this costs almost no disk.

```bash
# A normal topic (7 days)
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic churn.raw --partitions 3 --replication-factor 1 \
  --config retention.ms=604800000

# The dead-letter queue (no time limit)
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic churn.dlq --partitions 3 --replication-factor 1 \
  --config retention.ms=-1
```

(Replication factor is 1 because there is a single broker.)

---

## 10. Test that Kafka works (2-minute check)

Open two terminals.

**Terminal 1: consumer**

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic churn.raw --from-beginning \
  --property print.key=true
```

**Terminal 2: producer** (type `key:value`, then press Enter)

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic churn.raw \
  --property parse.key=true --property key.separator=:
> C10023:{"customer_id":"C10023","plan_id":3}
```

The message should appear in Terminal 1 and in Kafka UI (Topics → `churn.raw` → Messages). Press `Ctrl+C` to quit both. Run `bash scripts/reset_environment.sh` before a real replay, so NiFi does not read this test message.

```bash
# List topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# Describe one topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic churn.raw

# Consumer group lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --all-groups
```

**Test the producer** (after the topics exist and the dataset is in `data/raw/`):

```bash
python producer/producer.py --rate 10
```

Kafka UI should show messages arriving in `churn.raw`, starting with the earliest records of the dataset. Stop it with `Ctrl+C`.

---

## 11. Monitoring: consumer lag

**Lag** is the number of messages a consumer has not processed yet. A lag that keeps growing means the consumer is stuck or too slow. A lag of zero means it has caught up, which `scripts/verify_pipeline.py` uses to know when processing has finished.

- Easiest: Kafka UI → **Consumers** → pick the group (NiFi or Spark) → see the lag per partition.
- Command line: the `kafka-consumer-groups.sh --describe` command above.

---

## 12. Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| Containers keep restarting or die with exit code 137 | Not enough memory | Raise Docker memory (section 2). Start fewer services: `docker compose up -d kafka kafka-ui db`, then add the others |
| `port is already allocated` | Another program uses that port | Change the left port number in `docker-compose.yml` (section 4). For MySQL, set `DB_HOST_PORT=3307` in `.env` |
| Python producer cannot connect (`NoBrokersAvailable`, timeout) | Using `kafka:9092` from the laptop | Use `localhost:9094` from the laptop (section 5) |
| Producer says a dataset file is not found | The dataset is not in `data/raw/` | Put the files there (see `data/README.md`) and check the file names |
| NiFi or Spark cannot reach Kafka or the database | Using `localhost` inside a container | Use `kafka:9092` and `db:3306` (section 5) |
| NiFi page does not open | NiFi still starting | Wait 2 to 4 minutes; check `docker compose logs -f nifi` |
| NiFi login rejected | Password shorter than 12 characters, or `.env` changed after the first start | Fix `.env`, then `docker compose down -v` and start again |
| Topics are missing | The topic script was not run, or data was wiped | Run `bash scripts/create_topics.sh` |
| Database tables are missing | Init scripts only run on the first start with an empty volume | Run `bash scripts/init_db.sh` |
| `db` stays `starting` or `unhealthy` | MySQL is still initializing, or the data folder is corrupt | Wait 30 to 60 seconds and check `docker compose logs db`. If it keeps failing, `bash scripts/reset_environment.sh` |
| `Access denied for user` | `.env` changed after the first start (MySQL only reads the passwords on an empty volume) | Restore the old values, or `docker compose down -v` and start again |
| `Public Key Retrieval is not allowed` | JDBC URL is missing `allowPublicKeyRetrieval=true` | Use the JDBC URL from section 5 |
| `Unknown database` or `Table ... doesn't exist` from Spark | Wrong `DB_NAME`, or `init_db.sh` was not run | Check `.env`, then run `bash scripts/init_db.sh` |
| Timestamps are off by some hours | The JDBC URL is missing `connectionTimeZone=UTC`, or a column is `TIMESTAMP` | Use the JDBC URL from section 5 and `DATETIME(3)` columns |
| Spark cannot write to the database | MySQL driver missing or wrong | Check `scripts/submit_spark.sh` has `com.mysql:mysql-connector-j`, not the old `mysql-connector-java` |
| Spark shows no data after a restart | The job started from the latest offset, or an old checkpoint is in the way | Check `startingOffsets` is `earliest`, and reset the checkpoint folder with `bash scripts/reset_environment.sh` |
| Grafana data source test fails | Wrong host or login | Use host `db:3306` and the `DB_USER` login, not `localhost` and not root |
| Everything is slow | Laptop under memory pressure | Close browsers and heavy apps; reduce the producer `--rate`; stop services you do not need |
| Something is totally broken | Corrupt state | `bash scripts/reset_environment.sh` (deletes all data), then start again from section 3 |

When asking for help, send: the output of `docker compose ps`, the last 50 lines of the failing service (`docker compose logs --tail 50 <service>`), and your OS and RAM.

---

## 13. Rules for the team

1. **Do not edit `docker-compose.yml` directly on `main`.** Create a branch, open a pull request, and ask the team leader to review it.
2. **Never commit `.env`**, passwords, data dumps or the dataset files in `data/raw/`.
3. **Topic names, partitions, keys and retention are part of the contract.** Changes go through the team leader.
4. If you need an extra tool or a different configuration, ask in the group chat. Do not install things only on your machine, otherwise nobody can reproduce your results.

---

## 14. Environment checklist

Run these steps **from a clean state** (after `bash scripts/reset_environment.sh`):

- [ ] `docker compose up -d` starts every service without errors
- [ ] `db` reaches the `healthy` state
- [ ] `bash scripts/create_topics.sh` creates `churn.raw`, `churn.clean`, `churn.dlq` with 3 partitions each
- [ ] `churn.dlq` shows `retention.ms=-1` in `kafka-topics.sh --describe`
- [ ] `bash scripts/init_db.sh` creates the tables and views, and running it a second time does not fail
- [ ] The Kafka console test (section 10) works
- [ ] Kafka UI shows the topics and the test message
- [ ] NiFi, Spark master and Grafana pages open
- [ ] A Python script on your laptop can publish to `localhost:9094`
- [ ] The database accepts a connection on `localhost:<DB_HOST_PORT>` with the `.env` credentials (`DB_USER`, not root)
- [ ] This guide is clear: write down every step that confused you

---

## 15. Change log

| Date | Change | By |
|---|---|---|
| | v1: first version of the environment | Member 5 |
| 2026-11-14 | v2: database decided (MySQL 8.0). Added `DB_ROOT_PASSWORD` and `DB_HOST_PORT`, the MySQL JDBC URL and driver, the database section, and MySQL troubleshooting rows | Member 1 (docs), Member 5 to confirm against `docker-compose.yml` |
| 2026-11-18 | v3 (draft): new flow (dataset replayed by a producer, points-based flags in Spark). `churn.dlq` with no time limit, dataset step, producer replay command. Member names removed until roles are assigned | Docs draft, to be confirmed by the team |
| 2026-11-23 | v4: Implemented Docker Compose infrastructure (`docker-compose.yml`, `.env.example`). Configured KRaft Kafka dual listeners (9092/9094), MySQL 8.0 health checks and persistence, Spark Master/Worker (3.5.1 / Scala 2.12), NiFi, Kafka UI, and Grafana on unified `churn-network`. Verified connectivity. | Member 5 (Abobakr) |
