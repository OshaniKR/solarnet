"""
Daily tariff simulator -- Source B (batch), see SolarNet_Team_Project_Brief.pdf s.3.

Drops one CSV file per simulated day into TARIFF_OUTPUT_DIR, named
`tariff_YYYY-MM-DD.csv`, where the date is a purely cosmetic simulated calendar date
(SIM_START_DATE + day_index). The Airflow DAG does NOT rely on that date matching any
wall-clock date -- it treats the drop folder as a landing zone and picks up whichever
files it hasn't marked `.processed` yet (see airflow/dags/daily_billing_dag.py). This
decouples the simulator's cadence from the DAG's schedule and survives restarts of
either side cleanly.

The file is written to a .tmp path and then atomically os.replace()'d into place, so
a consumer (Airflow's sensor) can never observe a half-written CSV.
"""
import csv
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, "/app")
from common.logging_utils import get_logger, log_event
from common.metrics_utils import push_metrics

logger = get_logger("tariff-simulator")

SIM_DAY_SECONDS = int(os.environ.get("SIM_DAY_SECONDS", "300"))
NUM_HOUSEHOLDS = int(os.environ.get("NUM_HOUSEHOLDS", "40"))
OUTPUT_DIR = os.environ.get("TARIFF_OUTPUT_DIR", "/data/tariffs")
SIM_START_DATE = os.environ.get("SIM_START_DATE", "2026-01-01")
STATE_FILE = os.path.join(OUTPUT_DIR, ".sim_state.json")

TIERS = ["residential_standard", "residential_solar", "small_business"]
FIELDNAMES = ["household_id", "import_tariff_rate", "export_tariff_rate", "billing_tier", "subsidy_flag"]


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"day_index": 0}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def simulated_date_for(day_index: int):
    start = datetime.strptime(SIM_START_DATE, "%Y-%m-%d").date()
    return start + timedelta(days=day_index)


def generate_tariff_rows() -> list:
    rows = []
    for idx in range(1, NUM_HOUSEHOLDS + 1):
        rows.append({
            "household_id": f"HH{idx:03d}",
            "import_tariff_rate": round(random.uniform(0.28, 0.42), 4),
            "export_tariff_rate": round(random.uniform(0.08, 0.18), 4),
            "billing_tier": TIERS[idx % len(TIERS)],
            "subsidy_flag": idx % 5 == 0,
        })
    return rows


def write_tariff_file(day_index: int):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sim_date = simulated_date_for(day_index)
    filename = f"tariff_{sim_date.isoformat()}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    rows = generate_tariff_rows()

    tmp_path = filepath + ".tmp"
    with open(tmp_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, filepath)
    return filepath, len(rows)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    state = load_state()
    log_event(
        logger, "tariff_simulator_starting",
        sim_day_seconds=SIM_DAY_SECONDS, households=NUM_HOUSEHOLDS, resuming_at_day=state["day_index"],
    )

    while True:
        filepath, count = write_tariff_file(state["day_index"])
        log_event(logger, "tariff_file_written", file=filepath, rows=count, day_index=state["day_index"])
        push_metrics("tariff_simulator", {
            "tariff_sim_files_written_total": state["day_index"] + 1,
            "tariff_sim_last_file_rows": count,
        })
        state["day_index"] += 1
        save_state(state)
        time.sleep(SIM_DAY_SECONDS)


if __name__ == "__main__":
    main()
