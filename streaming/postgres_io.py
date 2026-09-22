"""
Postgres upsert helpers for the streaming job's foreachBatch sinks.

Plain psycopg2 rather than Spark's built-in JDBC writer, because a duplicate or
late-arriving row needs to hit raw_readings' UNIQUE constraint with ON CONFLICT DO
NOTHING instead of failing the whole microbatch on a constraint violation, and
live_zone_metrics needs an accumulating upsert keyed on (grid_zone, window_start).
"""
import os

import psycopg2
from psycopg2.extras import execute_values


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        dbname=os.environ.get("POSTGRES_DB", "solarnet"),
        user=os.environ.get("POSTGRES_USER", "solarnet"),
        password=os.environ.get("POSTGRES_PASSWORD", "solarnet"),
    )


def upsert_raw_readings(rows: list) -> None:
    """rows: (meter_id, household_id, grid_zone, power_consumption_kwh,
    solar_generation_kwh, net_kwh, is_exporting, event_timestamp) tuples."""
    if not rows:
        return
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO raw_readings (
                    meter_id, household_id, grid_zone, power_consumption_kwh,
                    solar_generation_kwh, net_kwh, is_exporting, event_timestamp
                ) VALUES %s
                ON CONFLICT (meter_id, event_timestamp) DO NOTHING
                """,
                rows,
            )
    finally:
        conn.close()


def upsert_zone_metrics(rows: list) -> None:
    """rows: (grid_zone, window_start, window_end, total_load_kwh, total_solar_kwh,
    solar_share_pct, exporting_households) tuples. Each row already carries the full
    accumulated total for that window (Spark's stateful streaming aggregation, not
    just this microbatch's slice), so DO UPDATE is a plain overwrite."""
    if not rows:
        return
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO live_zone_metrics (
                    grid_zone, window_start, window_end, total_load_kwh,
                    total_solar_kwh, solar_share_pct, exporting_households
                ) VALUES %s
                ON CONFLICT (grid_zone, window_start) DO UPDATE SET
                    window_end = EXCLUDED.window_end,
                    total_load_kwh = EXCLUDED.total_load_kwh,
                    total_solar_kwh = EXCLUDED.total_solar_kwh,
                    solar_share_pct = EXCLUDED.solar_share_pct,
                    exporting_households = EXCLUDED.exporting_households,
                    updated_at = now()
                """,
                rows,
            )
    finally:
        conn.close()
