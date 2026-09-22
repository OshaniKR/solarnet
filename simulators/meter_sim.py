"""
Smart meter simulator -- Source A (streaming), see SolarNet_Team_Project_Brief.pdf s.3.

Emits one JSON message per (household, tick) to the Kafka topic `meter_readings`,
keyed by household_id so per-household ordering is preserved within a partition.

Deliberately injects:
  * ~DUPLICATE_RATE duplicate messages (same key + payload sent twice)
  * ~DROP_RATE missing readings (silently skipped)
  * ~LATE_RATE late-arriving events (timestamp back-dated by 1-4 minutes)
so the streaming layer has something real to clean, dedupe and watermark.
"""
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

sys.path.insert(0, "/app")
from common.logging_utils import get_logger, log_event
from common.metrics_utils import push_metrics
from common.sim_clock import simulated_hour_of_day
from common.solar_model import consumption_kwh, solar_generation_kwh

logger = get_logger("meter-simulator")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = "meter_readings"

SIM_DAY_SECONDS = int(os.environ.get("SIM_DAY_SECONDS", "300"))
NUM_HOUSEHOLDS = int(os.environ.get("NUM_HOUSEHOLDS", "40"))
ZONES = ["Z1", "Z2", "Z3", "Z4"]

DUPLICATE_RATE = float(os.environ.get("DUPLICATE_RATE", "0.02"))
DROP_RATE = float(os.environ.get("DROP_RATE", "0.03"))
LATE_RATE = float(os.environ.get("LATE_RATE", "0.02"))
SOLAR_HOUSEHOLD_RATE = float(os.environ.get("SOLAR_HOUSEHOLD_RATE", "0.5"))

TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

HOUSEHOLDS = [
    {
        "household_id": f"HH{idx:03d}",
        "meter_id": f"MTR{idx:03d}",
        "grid_zone": ZONES[idx % len(ZONES)],
        "has_solar": random.random() < SOLAR_HOUSEHOLD_RATE,
    }
    for idx in range(1, NUM_HOUSEHOLDS + 1)
]


def build_reading(hh: dict, hour: float, ts: datetime) -> dict:
    consumption = consumption_kwh(hour)
    solar = solar_generation_kwh(hour) if hh["has_solar"] else 0.0
    return {
        "meter_id": hh["meter_id"],
        "household_id": hh["household_id"],
        "grid_zone": hh["grid_zone"],
        "power_consumption_kwh": consumption,
        "solar_generation_kwh": solar,
        "timestamp": ts.strftime(TS_FORMAT),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
    )
    log_event(
        logger, "meter_simulator_starting",
        households=NUM_HOUSEHOLDS, sim_day_seconds=SIM_DAY_SECONDS, kafka=KAFKA_BOOTSTRAP,
    )

    sent = duplicated = dropped = late = 0
    last_report = time.time()

    while True:
        hour = simulated_hour_of_day(SIM_DAY_SECONDS)
        now = datetime.now(timezone.utc)

        for hh in HOUSEHOLDS:
            if random.random() < DROP_RATE:
                dropped += 1
                continue

            is_late = random.random() < LATE_RATE
            event_ts = now - timedelta(seconds=random.uniform(60, 240)) if is_late else now
            if is_late:
                late += 1

            reading = build_reading(hh, hour, event_ts)
            producer.send(TOPIC, key=hh["household_id"], value=reading)
            sent += 1

            if random.random() < DUPLICATE_RATE:
                producer.send(TOPIC, key=hh["household_id"], value=reading)
                duplicated += 1

        producer.flush()

        if time.time() - last_report > 10:
            log_event(
                logger, "meter_batch_sent",
                sent=sent, duplicated=duplicated, dropped=dropped, late=late, sim_hour=round(hour, 2),
            )
            push_metrics("meter_simulator", {
                "meter_sim_messages_sent_total": sent,
                "meter_sim_duplicates_injected_total": duplicated,
                "meter_sim_dropped_readings_total": dropped,
                "meter_sim_late_events_total": late,
            })
            last_report = time.time()

        time.sleep(random.uniform(2, 5))


if __name__ == "__main__":
    main()
