"""
SolarNet serving API.

Endpoints (per the brief's section 6):
    GET /live/zones     -- latest windowed load / solar share / exporters per zone
    GET /alerts          -- recent alerts, newest first
    GET /report/latest   -- the most recent daily billing report exported by Airflow
    GET /metrics          -- Prometheus scrape endpoint for this process
    GET /health            -- basic liveness check

Also runs a background monitor loop implementing Alert 1 ("no data received in N
minutes") and Alert 2 ("zone solar share below X% during daytime") from the brief's
observability plan (section 7); both write rows into the `alerts` table and update
Prometheus gauges that observability/alert_rules.yml can fire on. Alert 3 ("Airflow
DAG failure or invalid tariff file") is written directly by the Airflow DAG -- see
airflow/dags/daily_billing_dag.py.
"""
import asyncio
import csv
import glob
import os
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

sys.path.insert(0, "/app")
from common.logging_utils import get_logger, log_event
from common.sim_clock import simulated_hour_of_day

logger = get_logger("api")

PG_CONF = dict(
    host=os.environ.get("POSTGRES_HOST", "postgres"),
    dbname=os.environ.get("POSTGRES_DB", "solarnet"),
    user=os.environ.get("POSTGRES_USER", "solarnet"),
    password=os.environ.get("POSTGRES_PASSWORD", "solarnet"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
)
REPORT_DIR = os.environ.get("REPORT_DIR", "/reports")
NO_DATA_ALERT_MINUTES = float(os.environ.get("NO_DATA_ALERT_MINUTES", "3"))
LOW_SOLAR_SHARE_PCT = float(os.environ.get("LOW_SOLAR_SHARE_PCT", "15"))
DAYTIME_START_HOUR = float(os.environ.get("DAYTIME_START_HOUR", "8"))
DAYTIME_END_HOUR = float(os.environ.get("DAYTIME_END_HOUR", "17"))
MONITOR_INTERVAL_SECONDS = float(os.environ.get("MONITOR_INTERVAL_SECONDS", "30"))

app = FastAPI(title="SolarNet API", version="1.0.0")

REQUEST_COUNTER = Counter("api_requests_total", "Total API requests", ["endpoint"])
LAST_READING_AGE = Gauge(
    "solarnet_last_reading_age_seconds", "Seconds since the most recent reading landed in raw_readings"
)
ZONE_SOLAR_SHARE = Gauge(
    "solarnet_zone_solar_share_pct", "Latest solar share percent per zone", ["grid_zone"]
)


def pg_conn():
    return psycopg2.connect(**PG_CONF)


@app.get("/health")
def health():
    REQUEST_COUNTER.labels(endpoint="/health").inc()
    return {"status": "ok"}


@app.get("/live/zones")
def live_zones():
    REQUEST_COUNTER.labels(endpoint="/live/zones").inc()
    sql = """
        SELECT DISTINCT ON (grid_zone) grid_zone, window_start, window_end,
               total_load_kwh, total_solar_kwh, solar_share_pct, exporting_households, updated_at
        FROM live_zone_metrics
        ORDER BY grid_zone, window_start DESC
    """
    with pg_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return {"zones": rows}


@app.get("/alerts")
def alerts(limit: int = 50):
    REQUEST_COUNTER.labels(endpoint="/alerts").inc()
    sql = """
        SELECT id, alert_type, severity, message, grid_zone, household_id, source, triggered_at, resolved
        FROM alerts ORDER BY triggered_at DESC LIMIT %s
    """
    with pg_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return {"alerts": rows}


@app.get("/report/latest")
def latest_report():
    REQUEST_COUNTER.labels(endpoint="/report/latest").inc()
    files = sorted(glob.glob(os.path.join(REPORT_DIR, "daily_report_*.csv")))
    if not files:
        raise HTTPException(status_code=404, detail="No daily report has been generated yet")
    latest = files[-1]
    with open(latest, newline="") as f:
        rows = list(csv.DictReader(f))
    return {"report_file": os.path.basename(latest), "households": rows}


@app.get("/metrics")
def metrics():
    data = generate_latest()
    return PlainTextResponse(content=data, media_type=CONTENT_TYPE_LATEST)


def write_alert(alert_type, message, severity="warning", grid_zone=None, source="api-monitor"):
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO alerts (alert_type, severity, message, grid_zone, source, triggered_at)
                   VALUES (%s, %s, %s, %s, %s, NOW())""",
                (alert_type, severity, message, grid_zone, source),
            )
            conn.commit()
    except Exception as e:
        log_event(logger, "alert_write_failed", error=str(e))


def check_no_data():
    """Alert rule 1: no data received in N minutes."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT MAX(event_timestamp) FROM raw_readings")
        (last_ts,) = cur.fetchone()
    if last_ts is None:
        return
    age_seconds = (datetime.now(timezone.utc) - last_ts).total_seconds()
    LAST_READING_AGE.set(age_seconds)
    if age_seconds > NO_DATA_ALERT_MINUTES * 60:
        write_alert(
            "NO_DATA",
            f"No meter readings received in the last {int(age_seconds)}s "
            f"(threshold {int(NO_DATA_ALERT_MINUTES * 60)}s)",
            severity="critical",
        )


def check_low_solar_share():
    """Alert rule 2: zone solar share below X% during (simulated) daytime."""
    hour = simulated_hour_of_day()
    if not (DAYTIME_START_HOUR <= hour <= DAYTIME_END_HOUR):
        return
    sql = """
        SELECT DISTINCT ON (grid_zone) grid_zone, solar_share_pct
        FROM live_zone_metrics ORDER BY grid_zone, window_start DESC
    """
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    for grid_zone, solar_share_pct in rows:
        ZONE_SOLAR_SHARE.labels(grid_zone=grid_zone).set(solar_share_pct or 0)
        if solar_share_pct is not None and solar_share_pct < LOW_SOLAR_SHARE_PCT:
            write_alert(
                "LOW_SOLAR_SHARE",
                f"Zone {grid_zone} solar share is {solar_share_pct:.1f}% "
                f"(threshold {LOW_SOLAR_SHARE_PCT}%) during simulated daytime hours",
                severity="warning",
                grid_zone=grid_zone,
            )


async def monitor_loop():
    while True:
        try:
            check_no_data()
            check_low_solar_share()
        except Exception as e:
            log_event(logger, "monitor_loop_error", error=str(e))
        await asyncio.sleep(MONITOR_INTERVAL_SECONDS)


@app.on_event("startup")
async def on_startup():
    log_event(logger, "api_starting")
    asyncio.create_task(monitor_loop())
