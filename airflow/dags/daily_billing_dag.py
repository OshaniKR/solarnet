"""
Daily billing DAG -- the Lambda batch layer (SolarNet_Team_Project_Brief.pdf section 6).

    1. wait_for_tariff_file  -- sensor: watch the drop folder for an unprocessed tariff CSV
    2. validate_tariff_file  -- schema + row-count + value checks (raises on failure)
    3. load_tariffs          -- upsert into `tariffs`
    4. aggregate_daily_usage -- sum imported/exported kWh per household from `raw_readings`
    5. compute_bills         -- join usage with tariffs, apply common.billing.compute_bill
    6. write_bills           -- upsert into `daily_bills`
    7. export_report         -- write reports/daily_report_<date>.csv (served by GET /report/latest)
    8. mark_processed        -- touch a `<file>.processed` marker so this file is never re-billed

DESIGN NOTE -- "which readings belong to this bill?":
    The simulated tariff filename carries a cosmetic simulated calendar date
    (see simulators/tariff_sim.py), but meter readings use real wall-clock timestamps.
    Rather than trying to keep two independently-running containers' clocks in exact
    lockstep, this DAG bills whatever landed in `raw_readings` in the last
    SIM_DAY_SECONDS of real time every time a new tariff file arrives, and labels the
    resulting bill with that file's simulated date. This is a deliberate simplification
    for the demo -- state it in the report. A production system would instead track a
    persisted high-watermark timestamp of the last successfully billed reading.

DESIGN NOTE -- "landing zone + processed marker":
    The sensor does not try to predict an exact expected filename per DAG run (fragile
    if the simulator's and Airflow's timing drift). It just picks the oldest tariff_*.csv
    in the folder that has no matching `.processed` marker file yet. This makes the batch
    layer idempotent and naturally catches up if a run is missed or the DAG is restarted.
"""
import csv
import glob
import os
import sys
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.sensors.python import PythonSensor

sys.path.insert(0, "/opt/airflow")
from common.billing import BillInputs, compute_bill
from common.logging_utils import get_logger, log_event
from common.metrics_utils import push_metrics

logger = get_logger("airflow-billing-dag")

TARIFF_DIR = os.environ.get("TARIFF_INPUT_DIR", "/data/tariffs")
REPORT_DIR = os.environ.get("REPORT_OUTPUT_DIR", "/reports")
SIM_DAY_SECONDS = int(os.environ.get("SIM_DAY_SECONDS", "300"))
REQUIRED_COLUMNS = {"household_id", "import_tariff_rate", "export_tariff_rate", "billing_tier", "subsidy_flag"}

PG_CONF = dict(
    host=os.environ.get("POSTGRES_HOST", "postgres"),
    dbname=os.environ.get("POSTGRES_DB", "solarnet"),
    user=os.environ.get("POSTGRES_USER", "solarnet"),
    password=os.environ.get("POSTGRES_PASSWORD", "solarnet"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
)


def pg_conn():
    return psycopg2.connect(**PG_CONF)


def write_alert(alert_type, message, severity="error", source="airflow"):
    try:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO alerts (alert_type, severity, message, source, triggered_at)
                   VALUES (%s, %s, %s, %s, NOW())""",
                (alert_type, severity, message, source),
            )
            conn.commit()
    except Exception as e:
        log_event(logger, "alert_write_failed", error=str(e))


def alert_on_failure(context):
    """Alert rule 3 (part 1): Airflow DAG/task failure -> row in `alerts` + a
    Prometheus gauge the alert_rules.yml BillingDagFailed rule watches."""
    task_id = context["task_instance"].task_id
    exception = context.get("exception")
    message = f"Task '{task_id}' failed: {exception}"
    write_alert("DAG_FAILURE", message, severity="critical", source="airflow")
    push_metrics("airflow_billing_dag", {"airflow_dag_last_run_status": 0})
    log_event(logger, "dag_task_failed", task_id=task_id, error=str(exception))


default_args = {
    "owner": "solarnet",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": alert_on_failure,
}


def _find_unprocessed_file(ti=None, **_):
    candidates = sorted(glob.glob(os.path.join(TARIFF_DIR, "tariff_*.csv")))
    for path in candidates:
        marker = path + ".processed"
        if not os.path.exists(marker):
            if ti is not None:
                ti.xcom_push(key="tariff_path", value=path)
            log_event(logger, "tariff_file_found", file=path)
            return True
    return False


@dag(
    dag_id="daily_billing_dag",
    description="SolarNet batch layer: validate tariffs, recompute daily bills, export the report.",
    schedule=timedelta(seconds=SIM_DAY_SECONDS),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["solarnet", "batch-layer"],
)
def daily_billing_dag():

    wait_for_tariff = PythonSensor(
        task_id="wait_for_tariff_file",
        python_callable=_find_unprocessed_file,
        poke_interval=10,
        timeout=SIM_DAY_SECONDS * 3,
        mode="reschedule",
    )

    @task
    def validate_tariff_file(ti=None) -> str:
        path = ti.xcom_pull(task_ids="wait_for_tariff_file", key="tariff_path")
        if not path or not os.path.exists(path):
            raise AirflowException(f"Tariff file not found: {path}")

        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = set(reader.fieldnames or [])
            rows = list(reader)

        missing_cols = REQUIRED_COLUMNS - fieldnames
        if missing_cols:
            write_alert("INVALID_TARIFF_FILE", f"{path} missing columns: {missing_cols}", source="airflow")
            raise AirflowException(f"Tariff file {path} missing columns: {missing_cols}")
        if not rows:
            write_alert("INVALID_TARIFF_FILE", f"{path} has no data rows", source="airflow")
            raise AirflowException(f"Tariff file is empty: {path}")

        for row in rows:
            if not row["household_id"]:
                raise AirflowException(f"Tariff file {path} has a row with empty household_id")
            try:
                imp = float(row["import_tariff_rate"])
                exp = float(row["export_tariff_rate"])
            except ValueError:
                raise AirflowException(f"Tariff file {path} has non-numeric rates in row {row}")
            if imp < 0 or exp < 0:
                raise AirflowException(f"Tariff file {path} has a negative rate in row {row}")

        log_event(logger, "tariff_file_validated", file=path, rows=len(rows))
        return path

    @task
    def load_tariffs(path: str) -> dict:
        sim_date = os.path.basename(path).replace("tariff_", "").replace(".csv", "")
        with open(path, newline="") as f:
            rows = list(csv.DictReader(f))

        values = [
            (
                r["household_id"], sim_date, float(r["import_tariff_rate"]),
                float(r["export_tariff_rate"]), r["billing_tier"],
                r["subsidy_flag"].strip().lower() in ("true", "1", "yes"),
            )
            for r in rows
        ]
        sql = """
            INSERT INTO tariffs
                (household_id, tariff_date, import_tariff_rate, export_tariff_rate, billing_tier, subsidy_flag)
            VALUES %s
            ON CONFLICT (household_id, tariff_date) DO UPDATE SET
                import_tariff_rate = EXCLUDED.import_tariff_rate,
                export_tariff_rate = EXCLUDED.export_tariff_rate,
                billing_tier = EXCLUDED.billing_tier,
                subsidy_flag = EXCLUDED.subsidy_flag
        """
        with pg_conn() as conn, conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values)
            conn.commit()

        log_event(logger, "tariffs_loaded", file=path, rows=len(values), sim_date=sim_date)
        return {"path": path, "sim_date": sim_date}

    @task
    def aggregate_daily_usage(ctx: dict) -> dict:
        sql = """
            SELECT household_id,
                   COALESCE(SUM(net_kwh) FILTER (WHERE net_kwh > 0), 0) AS imported_kwh,
                   COALESCE(SUM(-net_kwh) FILTER (WHERE net_kwh < 0), 0) AS exported_kwh
            FROM raw_readings
            WHERE event_timestamp >= NOW() - INTERVAL '1 second' * %s
            GROUP BY household_id
        """
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (SIM_DAY_SECONDS,))
            rows = cur.fetchall()
        usage = {r[0]: {"imported_kwh": float(r[1]), "exported_kwh": float(r[2])} for r in rows}
        log_event(logger, "usage_aggregated", households=len(usage), window_seconds=SIM_DAY_SECONDS)
        return {**ctx, "usage": usage}

    @task
    def compute_bills(ctx: dict) -> dict:
        sim_date = ctx["sim_date"]
        usage = ctx["usage"]
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT household_id, import_tariff_rate, export_tariff_rate, billing_tier, subsidy_flag "
                "FROM tariffs WHERE tariff_date = %s",
                (sim_date,),
            )
            tariff_rows = cur.fetchall()

        bills = []
        for household_id, import_rate, export_rate, tier, subsidy_flag in tariff_rows:
            u = usage.get(household_id, {"imported_kwh": 0.0, "exported_kwh": 0.0})
            result = compute_bill(BillInputs(
                household_id=household_id,
                imported_kwh=u["imported_kwh"],
                exported_kwh=u["exported_kwh"],
                import_tariff_rate=float(import_rate),
                export_tariff_rate=float(export_rate),
                subsidy_flag=bool(subsidy_flag),
                billing_tier=tier,
            ))
            bills.append({
                "household_id": result.household_id,
                "imported_kwh": result.imported_kwh,
                "exported_kwh": result.exported_kwh,
                "gross_amount": result.gross_amount,
                "subsidy_amount": result.subsidy_amount,
                "net_amount": result.net_amount,
                "billing_tier": result.billing_tier,
                "bill_date": sim_date,
                "import_tariff_rate": float(import_rate),
                "export_tariff_rate": float(export_rate),
                "subsidy_flag": bool(subsidy_flag),
            })
        log_event(logger, "bills_computed", bill_date=sim_date, households=len(bills))
        return {**ctx, "bills": bills}

    @task
    def write_bills(ctx: dict) -> dict:
        bills = ctx["bills"]
        sql = """
            INSERT INTO daily_bills
                (household_id, bill_date, imported_kwh, exported_kwh, import_tariff_rate,
                 export_tariff_rate, subsidy_flag, gross_amount, subsidy_amount, net_amount, billing_tier)
            VALUES %s
            ON CONFLICT (household_id, bill_date) DO UPDATE SET
                imported_kwh = EXCLUDED.imported_kwh,
                exported_kwh = EXCLUDED.exported_kwh,
                import_tariff_rate = EXCLUDED.import_tariff_rate,
                export_tariff_rate = EXCLUDED.export_tariff_rate,
                subsidy_flag = EXCLUDED.subsidy_flag,
                gross_amount = EXCLUDED.gross_amount,
                subsidy_amount = EXCLUDED.subsidy_amount,
                net_amount = EXCLUDED.net_amount,
                billing_tier = EXCLUDED.billing_tier
        """
        values = [
            (
                b["household_id"], b["bill_date"], b["imported_kwh"], b["exported_kwh"],
                b["import_tariff_rate"], b["export_tariff_rate"], b["subsidy_flag"],
                b["gross_amount"], b["subsidy_amount"], b["net_amount"], b["billing_tier"],
            )
            for b in bills
        ]
        with pg_conn() as conn, conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values)
            conn.commit()
        log_event(logger, "bills_written", bill_date=ctx["sim_date"], rows=len(values))
        return ctx

    @task
    def export_report(ctx: dict) -> dict:
        os.makedirs(REPORT_DIR, exist_ok=True)
        bills = ctx["bills"]
        sim_date = ctx["sim_date"]
        report_path = os.path.join(REPORT_DIR, f"daily_report_{sim_date}.csv")
        fieldnames = ["household_id", "billing_tier", "imported_kwh", "exported_kwh",
                      "gross_amount", "subsidy_amount", "net_amount"]
        with open(report_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for b in bills:
                writer.writerow({k: b[k] for k in fieldnames})

        log_event(logger, "report_exported", path=report_path, rows=len(bills))
        push_metrics("airflow_billing_dag", {
            "airflow_dag_last_run_status": 1,
            "airflow_dag_last_run_households_billed": len(bills),
        })
        return {**ctx, "report_path": report_path}

    @task
    def mark_processed(ctx: dict) -> None:
        marker = ctx["path"] + ".processed"
        with open(marker, "w") as f:
            f.write(datetime.utcnow().isoformat())
        log_event(logger, "tariff_file_marked_processed", file=ctx["path"])

    validated_path = validate_tariff_file()
    wait_for_tariff >> validated_path

    loaded_ctx = load_tariffs(validated_path)
    usage_ctx = aggregate_daily_usage(loaded_ctx)
    billed_ctx = compute_bills(usage_ctx)
    written_ctx = write_bills(billed_ctx)
    reported_ctx = export_report(written_ctx)
    mark_processed(reported_ctx)


daily_billing_dag()
