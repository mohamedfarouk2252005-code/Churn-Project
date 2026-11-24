# Team Docker & DevOps Guide
## Real-Time Customer Churn Analytics

**Audience:** All project members, including developers who are new to Docker  
**Owner:** Member 5 — Docker Infrastructure, Kafka & DevOps  
**Purpose:** Help every teammate start, inspect, use, and safely troubleshoot the shared local development environment.

> **Important scope note:** This guide describes the Docker configuration and test evidence supplied with the project archive. It does not claim that the complete business pipeline (producer → NiFi ETL → Spark streaming → SQL analytics → Grafana dashboards) is already implemented. Check the **Repository Snapshot Caveats** section before following steps that depend on scripts or folders not present in the supplied archive.

---

# 1. What Docker Does in This Project

Docker lets the team run the same infrastructure on different laptops without asking everyone to install Kafka, NiFi, Spark, MySQL, and Grafana directly on Windows or Linux.

A **container** is an isolated running service. A **Docker image** is the packaged software used to create a container. **Docker Compose** reads `docker-compose.yml` and starts the project's services together.

The supplied Compose configuration defines **seven containers on one Docker bridge network**:

| Compose service / container | Responsibility | Host access |
|---|---|---|
| `kafka` | Message broker; stores and distributes events | `localhost:9094` |
| `kafka-ui` | Browser UI for Kafka topics, messages, and consumer groups | http://localhost:8090 |
| `nifi` | ETL runtime; intended to consume raw events, clean/validate/enrich, and route them | https://localhost:8443/nifi |
| `spark-master` | Coordinates Spark applications | http://localhost:8080 and `localhost:7077` |
| `spark-worker` | Executes Spark tasks | http://localhost:8081 |
| `db` | MySQL database | `localhost:${DB_HOST_PORT:-3306}` |
| `grafana` | Dashboard and monitoring UI | http://localhost:3000 |

The Compose service names are also DNS names inside the Docker network. Containers should use service names to communicate with each other; they should not use `localhost` to reach another container.

## 1.1 Intended data pipeline

```text
Dataset
   |
   v
Python Producer (runs on a developer's laptop)
   |
   | localhost:9094
   v
Kafka topic: churn.raw
   |
   v
NiFi (inside Docker; Kafka address kafka:9092)
   |                         |
   | valid records           | invalid records
   v                         v
churn.clean                churn.dlq
   |
   v
Spark Structured Streaming
   |
   | JDBC to db:3306
   v
MySQL (db)
   |
   v
Grafana
```

This is the **intended application architecture** described in the project README and environment draft. The uploaded archive contains the infrastructure and a Spark connectivity smoke test, but it does not contain the producer, NiFi flow export, SQL files, or the full streaming application. Those stages must be supplied by their respective owners before the end-to-end pipeline can be considered ready.

---

# 2. Docker Concepts You Need

| Term | Meaning in this project |
|---|---|
| Image | Packaged software, e.g. `mysql:8.0.36` |
| Container | Running instance of an image, e.g. container `db` |
| Service | A Compose definition that describes how to run a container |
| Port mapping | Exposes a container port on the developer's laptop |
| Network | Lets containers find each other by service/container DNS name |
| Volume | Persistent storage that survives container removal |
| Bind mount | Maps a project folder/file from the laptop into a container |
| Health check | A command Docker uses to decide whether a service is ready |
| Environment variable | A configuration value supplied to a container, often from `.env` |
| `docker compose` | The command used to manage the complete multi-container project |

## 2.1 Network and addresses

The Compose file creates a bridge network named `churn-network`.

**From one container to another:**

- Kafka: `kafka:9092`
- MySQL: `db:3306`
- Spark master: `spark-master:7077`

**From an application running directly on the laptop:**

- Kafka: `localhost:9094`
- MySQL: `localhost:<DB_HOST_PORT>` (default `3306`)
- Browser interfaces: the `localhost` URLs in the service table.

Why are there two Kafka addresses? `kafka:9092` is the internal Docker listener. `localhost:9094` is the external listener advertised for programs running on the host machine. A Python producer on the laptop should use `localhost:9094`; a NiFi or Spark container should use `kafka:9092`.

> **Common mistake:** `localhost` inside a container means that same container. It does not mean the developer's laptop and it does not mean another container.

## 2.2 Ports

| Service | Container port | Host port in supplied Compose |
|---|---:|---:|
| Kafka external listener | 9094 | 9094 |
| Kafka UI | 8080 | 8090 |
| NiFi HTTPS UI | 8443 | 8443 |
| Spark master UI | 8080 | 8080 |
| Spark master RPC | 7077 | 7077 |
| Spark worker UI | 8081 | 8081 |
| MySQL | 3306 | `${DB_HOST_PORT}` (default 3306) |
| Grafana | 3000 | 3000 |

A port mapping follows this pattern: `HOST_PORT:CONTAINER_PORT`. If a local MySQL installation already uses port 3306, set `DB_HOST_PORT=3307` in `.env`. Other containers must still connect to `db:3306`; only the laptop-side port changes.

---

# 3. Prerequisites

Install and start:

1. Docker Desktop (Windows/macOS) or Docker Engine (Linux).
2. Docker Compose v2 (`docker compose`, not the old standalone `docker-compose` command).
3. Git, to clone and update the repository.
4. A terminal. On Windows, PowerShell is fine for Docker commands. Use WSL2 or Git Bash for Bash scripts when those scripts are available.
5. Enough memory and disk for the images and services. The existing environment draft recommends at least 8 GB RAM and 15 GB free disk; 16 GB RAM and 25 GB free disk are more comfortable.

Check the installation:

```bash
docker --version
docker compose version
docker info
```

`docker info` should return server information. If it says it cannot connect to the Docker daemon, start Docker Desktop / Docker Engine and wait until it is running.

---

# 4. First-Time Setup

Run commands from the repository root — the directory containing `docker-compose.yml`.

## Step 1 — Get the repository

```bash
git clone <REPOSITORY_URL>
cd Churn-Project
```

Replace `<REPOSITORY_URL>` with the team's actual Git URL. Do not guess the URL.

## Step 2 — Create your local environment file

Bash / WSL / Git Bash:

```bash
cp .env.example .env
```

PowerShell:

```powershell
Copy-Item .env.example .env
```

Open `.env` locally and set your own credentials. Do not send it in the team chat, upload it to a public repository, or commit it to Git.

The supplied `.env.example` defines:

| Variable | Purpose |
|---|---|
| `COMPOSE_PROJECT_NAME` | Compose project prefix |
| `DB_NAME` | MySQL database name |
| `DB_USER` / `DB_PASSWORD` | Normal application database login |
| `DB_ROOT_PASSWORD` | MySQL root password required by the image |
| `DB_HOST_PORT` | MySQL port exposed on the laptop |
| `NIFI_USERNAME` / `NIFI_PASSWORD` | NiFi single-user login; password must be at least 12 characters |
| `GRAFANA_USER` / `GRAFANA_PASSWORD` | Grafana administrator login |
| `SPARK_WORKER_MEMORY` | Spark worker memory limit |
| `SPARK_WORKER_CORES` | Spark worker core count |

The example file contains development defaults. Change them before sharing access beyond a trusted local development environment. Each teammate should have their own local `.env`.

## Step 3 — Validate the Compose configuration

```bash
docker compose config -q
```

No output and exit code 0 means Compose could parse the configuration. If it reports a missing variable, check that `.env` exists in the repository root and contains the required variable.

## Step 4 — Build and start the containers

```bash
docker compose up -d --build
```

The first run downloads images and builds the custom Spark image. It can take several minutes depending on the internet connection and machine.

## Step 5 — Check container status

```bash
docker compose ps
```

Expected services:

- `kafka`
- `kafka-ui`
- `nifi`
- `spark-master`
- `spark-worker`
- `db`
- `grafana`

The Kafka and MySQL health checks should eventually report `healthy`. Other services may show `running` / `Up` without a health status because the supplied Compose file does not define health checks for them.

If a service is restarting or exited, inspect its logs:

```bash
docker compose logs --tail 50 <service>
```

Example:

```bash
docker compose logs --tail 50 kafka
```

NiFi can take a few minutes to finish starting. MySQL also needs time to initialize its data directory on its first run.

---

# 5. Open the Service Interfaces

| Service | URL | Login |
|---|---|---|
| Kafka UI | http://localhost:8090 | No login configured in the supplied Compose file |
| NiFi | https://localhost:8443/nifi | `NIFI_USERNAME` / `NIFI_PASSWORD` from `.env` |
| Spark master | http://localhost:8080 | No login configured in the supplied Compose file |
| Spark worker | http://localhost:8081 | No login configured in the supplied Compose file |
| Grafana | http://localhost:3000 | `GRAFANA_USER` / `GRAFANA_PASSWORD` from `.env` |

NiFi uses HTTPS with a locally generated/self-signed certificate in this development setup. A browser certificate warning may appear. Only proceed when you are connecting to your own local development instance.

---

# 6. Kafka: Topics, Messages, and Connectivity

Kafka is the message broker. A **topic** is a named stream of messages. The project intends to use these topics:

| Topic | Purpose | Expected partitions | Retention |
|---|---|---:|---|
| `churn.raw` | Raw records published by the producer | 3 | 7 days |
| `churn.clean` | Validated and enriched records from NiFi | 3 | 7 days |
| `churn.dlq` | Invalid records routed by NiFi | 3 | No time limit |

Retention values:

- Seven days = `604800000` milliseconds.
- No time limit = `retention.ms=-1`.

The supplied test checklist successfully describes creating the three topics with these settings. The archive does **not** include `scripts/create_topics.sh`, so until that script is added, create the topics manually.

Run these commands inside the Kafka container:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic churn.raw --partitions 3 --replication-factor 1 \
  --config retention.ms=604800000

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic churn.clean --partitions 3 --replication-factor 1 \
  --config retention.ms=604800000

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic churn.dlq --partitions 3 --replication-factor 1 \
  --config retention.ms=-1
```

Replication factor is 1 because this local Compose stack runs one Kafka broker. This is suitable for local development, not a highly available production Kafka cluster.

## 6.1 List and inspect topics

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list
```

Describe a topic:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --describe --topic churn.raw
```

Check topic configuration:

```bash
docker exec kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:9092 \
  --entity-type topics --entity-name churn.dlq --describe
```

For `churn.dlq`, verify `retention.ms=-1`. For `churn.raw` and `churn.clean`, verify `retention.ms=604800000`.

## 6.2 Send a test message

Create a temporary test topic:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic smoke.test --partitions 1 --replication-factor 1
```

Start a producer:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic smoke.test
```

Type messages, one per line:

```text
hello 1
hello 2
hello 3
```

Press `Ctrl+C` to stop the producer.

Read the messages:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic smoke.test --from-beginning --timeout-ms 8000
```

Expected result: the three messages appear, followed by a message count of 3.

Delete the temporary topic after testing:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --delete --topic smoke.test
```

## 6.3 Which Kafka address should each teammate use?

| Teammate / application location | Bootstrap server |
|---|---|
| Python producer running on laptop | `localhost:9094` |
| NiFi running in Docker | `kafka:9092` |
| Spark running in Docker | `kafka:9092` |
| Kafka UI running in Docker | `kafka:9092` |

Do not copy the laptop address into a container's Kafka configuration.

---

# 7. NiFi: What the NiFi Teammate Needs to Do

NiFi is the ETL orchestration service. In the intended project design, it consumes messages from `churn.raw`, performs formatting cleanup, validates records, enriches valid records, and routes them:

- Valid records → `churn.clean`
- Invalid records → `churn.dlq`

## 7.1 Start and open NiFi

```bash
docker compose up -d nifi
docker compose logs --tail 50 nifi
```

Wait for the startup log to indicate that NiFi has started, then open:

https://localhost:8443/nifi

Sign in with the credentials in `.env`.

## 7.2 Addresses NiFi must use

When configuring a Kafka processor inside NiFi, the broker address must be:

```text
kafka:9092
```

It must **not** be `localhost:9094`, because NiFi is running inside Docker.

## 7.3 Intended NiFi responsibilities

The project README describes this order:

1. Consume raw customer JSON from `churn.raw`.
2. Clean formatting only (for example, trim whitespace and normalize formats). Do not silently repair invalid business values.
3. Validate the record against the shared data contract.
4. Add the agreed enrichment fields.
5. Route valid messages to `churn.clean`.
6. Route invalid messages to `churn.dlq` with the agreed error information.

The exact processor chain, processor properties, controller services, validation expressions, and routing relationships must come from the team's approved NiFi flow and data contract.

> **Snapshot limitation:** the supplied archive does not contain `nifi/churn_etl_flow.json` or the `nifi/` directory. Therefore this archive does not prove that the ETL flow is already imported or configured. The NiFi container starting successfully is not the same as the ETL pipeline being ready. The NiFi owner must provide and review the flow export and its setup instructions.

## 7.4 Check NiFi can reach Kafka

The supplied test checklist uses:

```bash
docker exec nifi bash -c \
  "echo > /dev/tcp/kafka/9092 && echo 'NiFi -> Kafka OK'"
```

Expected output:

```text
NiFi -> Kafka OK
```

This verifies TCP connectivity only. It does not prove that a NiFi processor is configured correctly or that messages are being transformed.

---

# 8. MySQL: Database Access and Checks

The Compose file defines MySQL `8.0.36` as service/container `db`.

Inside Docker, the database address is always:

```text
db:3306
```

From the laptop, use:

```text
localhost:<DB_HOST_PORT>
```

The database name, normal user, and passwords come from `.env`.

## 8.1 Open a MySQL shell as the application user

```bash
docker exec -it db mysql -u churn_user -p churn_db
```

When prompted, enter the value of `DB_PASSWORD` from your local `.env`. Replace `churn_user` and `churn_db` if you changed `DB_USER` or `DB_NAME`.

Useful SQL commands:

```sql
SELECT VERSION();
SHOW DATABASES;
SELECT DATABASE();
SHOW TABLES;
```

Exit MySQL:

```sql
exit
```

## 8.2 Check the server as root

```bash
docker exec -it db mysql -u root -p
```

Enter `DB_ROOT_PASSWORD` when prompted. Do not put real passwords in shared terminal screenshots or team messages.

## 8.3 Important database behavior

- The Compose file sets the server character set to `utf8mb4`.
- The Compose file sets the default server time zone to UTC (`+00:00`).
- A named volume called `churn_mysql_data` stores MySQL data.
- The `./sql` directory is intended to be mounted into `/docker-entrypoint-initdb.d`.
- The official MySQL image runs initialization SQL files on the first initialization of an empty data directory. Changing a `.sql` file later does not automatically rerun it.

> **Snapshot limitation:** the supplied archive does not contain the `sql/` directory or `scripts/init_db.sh`. The test checklist confirms that the database service and credentials were tested, but it does not provide the missing schema files. Do not assume project tables exist until the SQL owner supplies the schema and verifies it.

---

# 9. Spark: Master, Worker, and the Connectivity Smoke Test

The supplied Compose file defines:

- Spark `3.5.1` Master at `spark-master:7077`.
- Spark Master UI at http://localhost:8080.
- Spark Worker UI at http://localhost:8081.
- A custom image built from `Docker/spark/Dockerfile` in the supplied archive (see the path-case warning below).
- Python libraries `pyspark==3.5.1` and `pymysql` installed in that image.
- A persistent checkpoint volume named `churn_spark_checkpoints`.
- An Ivy cache volume named `churn_spark_ivy`.

## 9.1 Check Spark status

Open:

- Master: http://localhost:8080
- Worker: http://localhost:8081

The Master UI should report `ALIVE` and show the registered worker when the worker has connected.

## 9.2 Check Python dependencies

```bash
docker exec spark-master python3 --version
```

```bash
docker exec spark-master python3 -c \
  "import pyspark, pymysql; print('pyspark', pyspark.__version__, '| pymysql OK')"
```

The supplied test expects PySpark 3.5.1 and a successful `pymysql` import.

## 9.3 Check Spark container network access

```bash
docker exec spark-master python3 -c \
  "import socket; [print(h, p, 'OK', socket.create_connection((h,p),5) and '') for h,p in [('db',3306),('kafka',9092)]]"
```

Expected checks:

```text
db 3306 OK
kafka 9092 OK
```

This tests TCP reachability from the Spark Master container. It does not test credentials, SQL permissions, Kafka topic configuration, or the full streaming application.

## 9.4 Run the supplied Spark ↔ Kafka ↔ MySQL smoke test

The archive includes `Docker/spark/smoke_test.py`. The test reads messages from `smoke.test`, writes the count to MySQL over JDBC, reads it back, and asserts that the values match.

First, create `smoke.test` and publish three messages using Section 6.2. Then run:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.3.0 \
  /opt/spark-apps/smoke_test.py
```

Expected final output includes:

```text
KAFKA rows read: 3
MYSQL value read back: 3
SMOKE TEST PASSED
```

The first run may need to download the Spark Kafka connector and MySQL JDBC driver. It can take longer than later runs.

After a successful test, remove the temporary table and topic:

```bash
docker exec -it db mysql -u churn_user -p churn_db \
  -e "DROP TABLE IF EXISTS smoke_test;"
```

Enter the application user's password if prompted. Then:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --delete --topic smoke.test
```

Replace the database username and name with your `.env` values if they differ.

> **Important path check:** Compose mounts `./docker/spark` into `/opt/spark-apps`, while the supplied archive stores the files under `Docker/spark`. Windows commonly treats these paths as equivalent; case-sensitive Linux filesystems do not. Before relying on the smoke-test command on Linux, align the Compose path and the actual directory name.

---

# 10. Grafana: Connect It to MySQL

Open:

http://localhost:3000

Sign in using `GRAFANA_USER` and `GRAFANA_PASSWORD` from `.env`.

The intended data source is MySQL. In Grafana:

1. Open **Connections → Data sources**.
2. Add a **MySQL** data source.
3. Set Host to `db:3306` — not `localhost`.
4. Set Database to the value of `DB_NAME`.
5. Set User to `DB_USER`.
6. Set Password to `DB_PASSWORD`.
7. Save and test.

A successful connection verifies that Grafana can reach MySQL using those credentials. It does not prove that the analytical tables, dashboard panels, or queries exist.

> **Snapshot limitation:** the supplied archive does not contain `dashboard/`, a dashboard export, or the SQL schema. The data source can be configured, but the final project dashboard cannot be verified from this archive.

---

# 11. Daily Commands: The Team Cheat Sheet

Run these from the repository root.

| Task | Command |
|---|---|
| Start all services | `docker compose up -d` |
| Build and start | `docker compose up -d --build` |
| Show running services | `docker compose ps` |
| Stop services but keep containers/volumes | `docker compose stop` |
| Start stopped services | `docker compose start` |
| Restart one service | `docker compose restart nifi` |
| Follow one service's logs | `docker compose logs -f nifi` |
| Show last 50 log lines | `docker compose logs --tail 50 kafka` |
| Stop and remove containers/network (keep named volumes) | `docker compose down` |
| Validate Compose file | `docker compose config -q` |

Useful examples:

```bash
docker compose logs -f kafka
docker compose logs -f nifi
docker compose logs -f spark-master
docker compose logs -f db
```

Press `Ctrl+C` to stop following logs. This does not stop the container.

## 11.1 Start only selected services

For example, a teammate who is checking Kafka and MySQL can start:

```bash
docker compose up -d kafka kafka-ui db
```

A service may also start its dependencies according to the Compose configuration.

---

# 12. Persistence: What Survives a Restart?

The Compose file defines these named volumes:

| Volume | Stores |
|---|---|
| `churn_kafka_data` | Kafka broker data |
| `churn_mysql_data` | MySQL database files |
| `churn_grafana_data` | Grafana data |
| `churn_nifi_state` | NiFi state |
| `churn_nifi_conf` | NiFi configuration |
| `churn_spark_checkpoints` | Spark checkpoint data |
| `churn_spark_ivy` | Downloaded Spark package cache |

The important distinction:

- `docker compose down` removes the containers and Compose network, but normally keeps named volumes.
- `docker compose up -d` recreates containers and reuses those volumes.
- `docker compose down -v` removes the Compose-managed named volumes as well. This can delete Kafka data, MySQL data, Grafana state, NiFi state/configuration, Spark checkpoints, and cached packages.

> **Danger:** Do not run `docker compose down -v` just because a service has an error. It is a destructive reset. Confirm with the team and back up any data that matters first.

---

# 13. Troubleshooting

| Symptom | What to check |
|---|---|
| `Cannot connect to the Docker daemon` | Start Docker Desktop / Docker Engine and wait until it is ready. |
| `Copy .env.example to .env first` or missing-variable error | Confirm `.env` exists in the repository root and contains all required variables. |
| `docker compose config` fails | Read the exact missing variable or YAML error; check `.env` and indentation. |
| `port is already allocated` | Another application uses the host port. Change the host-side port mapping; for MySQL, set `DB_HOST_PORT=3307`. |
| Kafka is restarting / unhealthy | Run `docker compose logs --tail 100 kafka`; check Docker resources and Kafka volume state before considering a reset. |
| MySQL is still starting | Wait and run `docker compose ps`; inspect `docker compose logs --tail 100 db`. |
| MySQL `Access denied` | Check the correct username/password and remember that changing `.env` does not rewrite credentials in an already initialized MySQL volume. |
| NiFi page does not open | Wait for startup and inspect `docker compose logs --tail 100 nifi`; use HTTPS on port 8443. |
| NiFi login rejected | Check `.env` and the 12-character minimum for the password. Existing NiFi state may retain prior credentials. |
| Kafka producer on laptop cannot connect | Use `localhost:9094`, not `kafka:9092`. |
| NiFi/Spark cannot reach Kafka | From containers use `kafka:9092`. |
| Spark cannot reach MySQL | From containers use `db:3306`, not `localhost`. |
| Spark cannot find Python or PyMySQL | Check the custom Spark image build and rebuild after correcting the Dockerfile/build path. |
| Spark cannot find `smoke_test.py` | Check the Compose bind-mount path and the case-sensitive `Docker/spark` vs `docker/spark` directory name. |
| Spark package download fails | Check internet access and inspect the Ivy cache/logs; retry after network access is restored. |
| Grafana MySQL data source fails | Use host `db:3306` and the application DB credentials, not `localhost` or root. |
| A page is unavailable but container is Up | Inspect the service logs; container state alone does not prove that its application is ready. |
| `no space left on device` | Check Docker disk usage before deleting anything. Do not blindly prune volumes that may contain project data. |

When asking the team for help, send:

```bash
docker compose ps
docker compose logs --tail 50 <service-name>
```

Also include your operating system, Docker version, and the exact command/error. Never include passwords or the contents of `.env`.

---

# 14. Team Workflows by Responsibility

## 14.1 Infrastructure / DevOps owner

1. Keep `docker-compose.yml`, `.env.example`, and this guide aligned.
2. Review image versions, ports, health checks, network, bind mounts, and named volumes.
3. Validate with `docker compose config -q`.
4. Build and start with `docker compose up -d --build`.
5. Verify all seven services using `docker compose ps`.
6. Run the relevant Kafka, MySQL, Spark, and NiFi connectivity tests.
7. Document any configuration change and tell the team before merging it.

## 14.2 Kafka / Producer owner

1. Start Kafka and Kafka UI.
2. Create the three project topics with the approved partition and retention settings.
3. Verify them using `kafka-topics.sh --list` and `--describe`.
4. Configure a laptop-based producer to use `localhost:9094`.
5. Use `customer_id` as the message key if required by the approved data contract.
6. Publish test messages and inspect them in Kafka UI.
7. Coordinate with the NiFi owner before sending real project data.

The producer code and its dependencies are not included in the supplied archive, so its exact CLI and Python package setup must be documented by its owner.

## 14.3 NiFi / ETL owner

1. Start NiFi and open its HTTPS UI.
2. Configure Kafka connectivity using `kafka:9092`.
3. Import the team's approved NiFi flow when the export is available.
4. Configure controller services and any lookup files required by that flow.
5. Verify that valid records reach `churn.clean`.
6. Verify that invalid records reach `churn.dlq` with the agreed error fields.
7. Document processor configuration and how to start/stop the flow.

A running NiFi container is only the runtime. It is not proof that the ETL flow has been imported or started.

## 14.4 Spark owner

1. Confirm the Spark Master and Worker are alive.
2. Use `kafka:9092` for Kafka and `db:3306` for MySQL from Spark containers.
3. Ensure the Spark Kafka connector matches Spark 3.5.1 / Scala 2.12.
4. Use `com.mysql:mysql-connector-j` for the MySQL JDBC driver.
5. Keep checkpoint locations stable and document how a deliberate replay/reset should work.
6. Run the supplied smoke test before testing the full streaming application.

The supplied archive includes only `smoke_test.py`, not the full `spark/` application described in the project structure.

## 14.5 SQL / Database owner

1. Confirm the MySQL service is healthy.
2. Supply and maintain the SQL schema, indexes, views, and seed files.
3. Document which scripts initialize the schema and whether they are safe to rerun.
4. Tell Spark and Grafana owners about approved table/column changes.
5. Do not assume that editing a mounted SQL file reruns MySQL initialization.

## 14.6 Grafana / Dashboard owner

1. Open Grafana at `http://localhost:3000`.
2. Configure the MySQL data source with host `db:3306`.
3. Use the normal application database user.
4. Import the approved dashboard export or create panels from reviewed SQL queries.
5. Verify panels against the actual database schema.

The dashboard export and query files are not present in the supplied archive.

---

# 15. Safe Reset and Recovery

There are different levels of reset. Choose the least destructive action that solves the problem.

### Level 1 — Restart one service

```bash
docker compose restart <service>
```

Example:

```bash
docker compose restart nifi
```

### Level 2 — Stop and start the stack, preserving volumes

```bash
docker compose down
docker compose up -d
```

### Level 3 — Rebuild images

```bash
docker compose up -d --build
```

### Level 4 — Full destructive reset

Only when the team explicitly intends to delete local persistent state:

```bash
docker compose down -v
docker compose up -d --build
```

This deletes the named volumes listed in Section 12. It can erase Kafka topics/messages, MySQL data, Grafana state, NiFi state/configuration, and Spark checkpoints. Recreate topics and database schema afterward using the team's approved scripts or SQL files.

Do not run a full reset on a teammate's machine or shared environment without their approval.

---

# 16. Before You Say “My Environment Is Ready”

- [ ] Docker Engine is running.
- [ ] `.env` exists locally and is not committed.
- [ ] `docker compose config -q` succeeds.
- [ ] `docker compose up -d --build` completes.
- [ ] All seven expected services appear in `docker compose ps`.
- [ ] Kafka and MySQL become healthy.
- [ ] Kafka UI opens.
- [ ] The three project topics exist with the correct retention.
- [ ] A test message can be produced and consumed.
- [ ] NiFi UI opens and can reach `kafka:9092`.
- [ ] Spark Master and Worker are alive.
- [ ] Spark can reach `kafka:9092` and `db:3306`.
- [ ] The Spark ↔ Kafka ↔ MySQL smoke test passes.
- [ ] Grafana opens and its MySQL data source can connect.
- [ ] The team has supplied the missing NiFi flow, SQL schema, producer, and application scripts before claiming the full business pipeline is ready.

---

# 17. Repository Snapshot Caveats (Read Before Using This Guide)

The supplied ZIP was inspected as an actual file snapshot. The following are facts about that snapshot, not assumptions about what may exist in another branch or on another teammate's machine:

1. `docker-compose.yml` uses `./docker/spark` for the Spark build context and bind mount, while the archive contains `Docker/spark/` with an uppercase `D`. Align these paths before supporting case-sensitive Linux hosts.
2. Compose references `./sql` and `./nifi/lookup_tables`, but those directories are absent from the supplied archive.
3. The archive does not contain `scripts/create_topics.sh`, `scripts/init_db.sh`, `scripts/reset_environment.sh`, or `scripts/submit_spark.sh`. This guide therefore gives manual Kafka topic commands and the explicit smoke-test command instead of pretending those scripts are available.
4. The archive does not contain `nifi/churn_etl_flow.json`, the producer code, the full Spark streaming application, SQL schema files, or Grafana dashboard exports.
5. `docs/environment.md` and `PROJECT_STRUCTURE.md` describe a broader planned repository than the actual files present in this ZIP. Treat those descriptions as project plans until the corresponding files are delivered and tested.
6. The ZIP contains a `.env` file even though `.env` is ignored by the repository's `.gitignore`. Do not redistribute that file. If it contains real credentials, rotate them and keep the replacement local. This guide intentionally does not reproduce any `.env` contents.
7. The Git metadata in the supplied ZIP shows the Docker Compose file, `.env.example`, and `Docker/` directory as untracked in that snapshot, while `docs/environment.md` is modified. Confirm the intended branch and commit these deliverables through the team's normal review process before telling everyone to pull them.

## What is verified versus what is not

| Item | Evidence in supplied materials | Status |
|---|---|---|
| Seven-service Compose design | `docker-compose.yml` | Configuration inspected |
| Kafka dual listeners and KRaft settings | `docker-compose.yml` + test checklist | Configuration inspected; test checklist reports success |
| MySQL health check, UTC, charset, volume | `docker-compose.yml` + test checklist | Configuration inspected; test checklist reports success |
| Spark custom image and PyMySQL | `Docker/spark/Dockerfile` + test checklist | Files inspected; test checklist reports success |
| Spark reads Kafka and writes/reads MySQL | `Docker/spark/smoke_test.py` + test checklist | Test implementation inspected; checklist reports pass |
| NiFi-to-Kafka TCP connectivity | Test checklist command | Checklist reports success; no ETL flow supplied |
| Kafka topics and retention | Test checklist commands/results | Checklist reports success; topic-creation script absent |
| SQL schema initialization | No SQL files in ZIP | Not verifiable from this snapshot |
| Full NiFi ETL flow | No flow export in ZIP | Not verifiable from this snapshot |
| Full producer-to-dashboard pipeline | Application components missing | Not verifiable from this snapshot |

---

# 18. Working Rules for Everyone

1. Run Docker commands from the repository root.
2. Do not commit `.env`, passwords, private datasets, or local database dumps.
3. Do not use `localhost` for container-to-container connections.
4. Do not change ports, topic names, retention, image versions, or shared environment variables without telling the team.
5. Do not run `docker compose down -v` unless you intend to erase local persistent data.
6. Update the relevant README/documentation whenever a service command or configuration changes.
7. Test your stage locally and ask another teammate to reproduce it before marking it complete.
8. Report errors with the command, output, service logs, OS, and Docker version — never with secrets.

---

**End of guide.** Keep this document synchronized with the actual Compose file, scripts, NiFi flow, SQL schema, and application code as they are merged into the repository.
