"""
Exercises the actual filter_valid() / dedup logic from streaming/spark_job.py
against a local Spark session. Skips cleanly if pyspark isn't installed --
run `pip install -r tests/requirements.txt` first if you want this one to run.
"""
import importlib.util
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed; skipping Spark transform tests")

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, to_timestamp


def _load_spark_job_module():
    path = os.path.join(os.path.dirname(__file__), "..", "streaming", "spark_job.py")
    spec = importlib.util.spec_from_file_location("spark_job", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("solarnet-tests").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def sj():
    return _load_spark_job_module()


def _sample_rows():
    return [
        Row(meter_id="M1", household_id="HH001", grid_zone="Z1",
            power_consumption_kwh=2.0, solar_generation_kwh=5.0,
            timestamp="2026-01-01T12:00:00.000000", raw_value="{}"),
        Row(meter_id="M2", household_id="HH002", grid_zone="Z1",
            power_consumption_kwh=1.5, solar_generation_kwh=0.0,
            timestamp="2026-01-01T12:00:00.000000", raw_value="{}"),
        # invalid: negative consumption
        Row(meter_id="M3", household_id="HH003", grid_zone="Z2",
            power_consumption_kwh=-1.0, solar_generation_kwh=0.0,
            timestamp="2026-01-01T12:00:00.000000", raw_value="bad"),
        # invalid: missing meter_id
        Row(meter_id=None, household_id="HH004", grid_zone="Z2",
            power_consumption_kwh=1.0, solar_generation_kwh=0.0,
            timestamp="2026-01-01T12:00:00.000000", raw_value="bad2"),
    ]


def test_filter_valid_splits_good_and_bad_records(spark, sj):
    df = spark.createDataFrame(_sample_rows())
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), sj.TS_PATTERN))

    valid = sj.filter_valid(df)
    invalid = df.exceptAll(valid)

    assert valid.count() == 2
    assert invalid.count() == 2
    assert set(r["household_id"] for r in valid.collect()) == {"HH001", "HH002"}


def test_dedup_on_meter_and_timestamp(spark, sj):
    rows = _sample_rows()[:1] * 2  # exact duplicate, like meter_sim's injected duplicates
    df = spark.createDataFrame(rows).withColumn("timestamp", to_timestamp(col("timestamp"), sj.TS_PATTERN))

    deduped = df.dropDuplicates(["meter_id", "timestamp"])
    assert deduped.count() == 1


def test_net_kwh_and_export_flag(spark):
    df = spark.createDataFrame(
        [("HH001", 5.0, 8.0), ("HH002", 6.0, 1.0)],
        ["household_id", "power_consumption_kwh", "solar_generation_kwh"],
    )
    result = df.withColumn(
        "net_kwh", col("power_consumption_kwh") - col("solar_generation_kwh")
    ).withColumn("is_exporting", col("net_kwh") < 0)

    rows = {r["household_id"]: r for r in result.collect()}
    assert rows["HH001"]["net_kwh"] == pytest.approx(-3.0)
    assert rows["HH001"]["is_exporting"] is True
    assert rows["HH002"]["net_kwh"] == pytest.approx(5.0)
    assert rows["HH002"]["is_exporting"] is False
