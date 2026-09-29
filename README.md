# SolarNet

A Lambda-architecture data platform for **Smart Grid Energy Monitoring & Billing**
with solar net billing — built for the **EC8203 Applied Big Data Engineering**
mini project (Use Case 3: Smart Grid Energy Monitoring & Billing).

It ingests simulated smart-meter readings (streaming) and a daily tariff file
(batch), cleans and de-duplicates them, serves live grid-load / solar-share
figures per zone, and recomputes each household's net-billing bill once a day —
fully containerised, observable, and running end to end with one command.


## Repository layout

```
solarnet/
├── docker-compose.yml    One command to run everything
├── .env.example           Copy to .env to override defaults
├── common/                 Shared pure logic: billing math, solar curves, simulated clock, logging, metrics
├── simulators/               Source A (meter_sim.py) and Source B (tariff_sim.py)
├── streaming/                 Spark Structured Streaming job — the speed layer
├── airflow/dags/                The daily billing DAG — the batch layer
├── api/                           FastAPI serving layer
├── sql/schema.sql                  PostgreSQL schema, auto-applied on first start
├── observability/                   prometheus.yml + alert_rules.yml
├── tests/                             pytest unit tests (billing, solar model, Spark transforms)
├── docs/architecture.md                 Architecture notes and diagram source
├── scripts/reset.sh                      Wipe state and start clean
├── data/tariffs/                          Where the tariff simulator drops daily CSVs
└── reports/                                Where the Airflow DAG exports the daily report
```

## Architecture

A Lambda architecture with two independent processing paths that converge on
one PostgreSQL store and one serving layer:

- **Speed layer:** `meter-simulator` → Kafka (`meter_readings`) → Spark
  Structured Streaming (`streaming/spark_job.py`) → `raw_readings` +
  `live_zone_metrics`. Invalid readings are routed to a dead-letter topic
  (`meter_readings_dlq`) instead of being dropped.
- **Batch layer:** `tariff-simulator` → daily CSV drop (`data/tariffs/`) →
  Airflow (`daily_billing_dag`) → `tariffs` + `daily_bills` + an exported
  report.
- **Serving layer:** FastAPI (`api/main.py`) exposes both layers through
  `/live/zones`, `/report/latest`, `/alerts` and `/metrics`.
- **Observability:** structured JSON logs everywhere, Prometheus + Pushgateway
  for metrics, and three alert rules covering missing data, low renewable
  share, and DAG failure.

Why Lambda over Kappa, and the full component-by-component justification, are
in the project report — see `docs/architecture.md` for the diagram source and
talking points.

## Prerequisites

- **Docker Desktop** (or Docker Engine) with **Compose V2** — the `docker
  compose` command, not the old standalone `docker-compose`. Check with:
  `docker compose version`
- About **6 GB of free RAM** for Docker (Kafka + Zookeeper + Spark + Airflow +
  Postgres + Prometheus all run at once).
- A free few minutes on first run — Kafka/Spark/Airflow images need to
  download once. Subsequent starts are much faster since everything is
  cached locally.
- Ports **9092, 5432, 9090, 9091, 8080, 8000** free on your machine.

No local installs of Kafka, Spark, Airflow or Postgres are needed — everything
runs inside containers.

## Quick start

1. **Clone and enter the project.**
   ```bash
   git clone <this-repo-url>
   cd solarnet
   ```

2. **Copy the env file** (defaults already match `docker-compose.yml`, so
   this is optional unless you want to change household count, simulated day
   length, or alert thresholds):
   ```bash
   cp .env.example .env
   ```

3. **Build and start everything:**
   ```bash
   docker compose up --build -d
   ```
   This builds four custom images (`meter-simulator`, `tariff-simulator`,
   `spark-job`, `api`) plus Airflow, and pulls the rest (Kafka, Zookeeper,
   Postgres, Prometheus, Pushgateway). Give it 1–2 minutes for Kafka to
   become healthy on first boot. If an image pull times out, just re-run the
   same command — Docker resumes from where it stopped.

4. **Check everything is up:**
   ```bash
   docker compose ps
   ```
   You should see `zookeeper`, `kafka`, `kafka-init` (exited — this is a
   one-shot job, that's correct), `postgres`, `pushgateway`, `prometheus`,
   `meter-simulator`, `tariff-simulator`, `spark-job`, `airflow`, and `api`
   all `Up`, with `kafka`/`postgres`/`api` marked `healthy`.

5. **Watch the logs** (optional, but reassuring the first time):
   ```bash
   docker compose logs -f meter-simulator tariff-simulator spark-job
   ```
   You should see structured JSON log lines like `meter_batch_sent`,
   `tariff_file_written`, `raw_batch_processed`, and
   `zone_metrics_batch_processed` appearing every 10–15 seconds.

6. **Check the live API:**
   ```bash
   curl http://localhost:8000/live/zones
   ```
   Within about a minute of startup you should see grid zones with non-zero
   `total_load_kwh`. Example real output:
   ```json
   {"zones":[{"grid_zone":"Z1","window_start":"2026-09-27T18:09:00+00:00",
     "window_end":"2026-09-27T18:10:00+00:00","total_load_kwh":29.7471,
     "total_solar_kwh":0.381,"solar_share_pct":1.28, ...}]}
   ```

7. **Open the Airflow UI** at [http://localhost:8080](http://localhost:8080).
   `daily_billing_dag` is unpaused and scheduled automatically (every
   `SIM_DAY_SECONDS`, 5 minutes by default) — you don't need to trigger it by
   hand. Login is `admin` with a randomly generated password, retrieved with:
   ```bash
   docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
   ```

8. **After the first DAG run completes** (green in the UI), check the
   report and the alerts feed:
   ```bash
   curl http://localhost:8000/report/latest
   curl http://localhost:8000/alerts
   ```

9. **Prometheus** is at [http://localhost:9090](http://localhost:9090) — try
   the query `solarnet_last_reading_age_seconds`, or check
   [http://localhost:9090/alerts](http://localhost:9090/alerts) to see the
   three alert rules from `observability/alert_rules.yml`.

## Running the tests

```bash
pip install -r tests/requirements.txt   # pytest + pyspark
pytest tests/ -v
```
Covers the billing math, the solar/consumption curves, and the Spark
validation/de-duplication transforms run against a real local Spark session.
If you don't want to install PySpark locally just to run tests, drop it from
`tests/requirements.txt` — the Spark tests skip themselves cleanly when
PySpark isn't installed.


## Resetting between runs

```bash
./scripts/reset.sh
docker compose up --build -d
```
This stops everything, drops the Postgres volume (so the schema re-applies
cleanly), and clears generated tariff/report files.

