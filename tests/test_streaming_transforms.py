"""
Unit tests for the streaming job's DataFrame transformations (streaming/transforms.py).

Runs against small static DataFrames with a local[1] SparkSession -- no Kafka, no
Postgres, no running containers required. window()/withWatermark() behave the same
on a static DataFrame as a streaming one, which is what makes these pure functions
testable in isolation from the live Kafka source and Postgres sinks wired up in
streaming/spark_job.py.
"""
import json
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from streaming.transforms import (
    aggregate_zone_metrics,
    dedup_readings,
    enrich_readings,
    parse_readings,
    split_valid_invalid,
    to_dlq_payload,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("solarnet-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _reading(meter_id="MTR001", household_id="HH001", grid_zone="Z1",
             power=1.0, solar=0.2, ts="2026-01-01T10:00:00.000000"):
    return json.dumps({
        "meter_id": meter_id,
        "household_id": household_id,
        "grid_zone": grid_zone,
        "power_consumption_kwh": power,
        "solar_generation_kwh": solar,
        "timestamp": ts,
    })


def _kafka_df(spark, messages):
    """messages: list of (key, value_json_str) tuples, mirroring the Kafka source's
    (key, value) string columns."""
    return spark.createDataFrame(messages, ["key", "value"])


def test_parse_readings_flattens_and_parses_timestamp(spark):
    df = _kafka_df(spark, [("HH001", _reading())])
    row = parse_readings(df).collect()[0]
    assert row.meter_id == "MTR001"
    assert row.event_timestamp == datetime(2026, 1, 1, 10, 0, 0)


def test_split_valid_invalid_catches_malformed_json(spark):
    df = _kafka_df(spark, [("HH001", "not-json-at-all")])
    valid_df, invalid_df = split_valid_invalid(parse_readings(df))
    assert valid_df.count() == 0
    assert invalid_df.collect()[0].invalid_reason == "malformed_json"


def test_split_valid_invalid_catches_negative_values(spark):
    df = _kafka_df(spark, [("HH001", _reading(power=-1.0))])
    valid_df, invalid_df = split_valid_invalid(parse_readings(df))
    assert valid_df.count() == 0
    assert invalid_df.collect()[0].invalid_reason == "invalid_consumption_value"


def test_split_valid_invalid_catches_missing_field(spark):
    bad = json.dumps({"meter_id": "MTR001", "power_consumption_kwh": 1.0})
    df = _kafka_df(spark, [("HH001", bad)])
    valid_df, invalid_df = split_valid_invalid(parse_readings(df))
    assert valid_df.count() == 0
    assert invalid_df.collect()[0].invalid_reason == "missing_required_field"


def test_split_valid_invalid_passes_good_reading(spark):
    df = _kafka_df(spark, [("HH001", _reading())])
    valid_df, invalid_df = split_valid_invalid(parse_readings(df))
    assert invalid_df.count() == 0
    assert valid_df.count() == 1


def test_enrich_readings_computes_net_and_export_flag(spark):
    df = _kafka_df(spark, [("HH001", _reading(power=1.0, solar=3.0))])
    valid_df, _ = split_valid_invalid(parse_readings(df))
    row = enrich_readings(valid_df).collect()[0]
    assert row.net_kwh == -2.0
    assert row.is_exporting is True


def test_dedup_readings_drops_exact_duplicate(spark):
    df = _kafka_df(spark, [("HH001", _reading()), ("HH001", _reading())])
    valid_df, _ = split_valid_invalid(parse_readings(df))
    enriched = enrich_readings(valid_df)
    assert dedup_readings(enriched).count() == 1


def test_to_dlq_payload_carries_raw_value_and_reason(spark):
    df = _kafka_df(spark, [("HH001", "not-json-at-all")])
    _, invalid_df = split_valid_invalid(parse_readings(df))
    payload = to_dlq_payload(invalid_df).collect()[0]
    assert payload.key == "HH001"
    body = json.loads(payload.value)
    assert body["invalid_reason"] == "malformed_json"
    assert body["raw_value"] == "not-json-at-all"


def test_aggregate_zone_metrics_sums_per_zone(spark):
    messages = [
        ("HH001", _reading(meter_id="MTR001", household_id="HH001", grid_zone="Z1", power=2.0, solar=0.0)),
        ("HH002", _reading(meter_id="MTR002", household_id="HH002", grid_zone="Z1", power=1.0, solar=3.0)),
        ("HH003", _reading(meter_id="MTR003", household_id="HH003", grid_zone="Z2", power=1.0, solar=0.0)),
    ]
    df = _kafka_df(spark, messages)
    valid_df, _ = split_valid_invalid(parse_readings(df))
    enriched = enrich_readings(valid_df)
    metrics = {r.grid_zone: r for r in aggregate_zone_metrics(enriched).collect()}

    z1 = metrics["Z1"]
    assert z1.total_load_kwh == 3.0
    assert z1.total_solar_kwh == 3.0
    assert z1.exporting_households == 1  # only HH002 (solar 3.0 > power 1.0)

    z2 = metrics["Z2"]
    assert z2.exporting_households == 0
