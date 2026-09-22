"""
SolarNet speed layer: Spark Structured Streaming job.

Reads raw meter readings from Kafka (`meter_readings`), validates and deduplicates
them, and fans out to three sinks:
  1. `meter_readings_dlq` (Kafka)    -- malformed / invalid readings, with a reason
  2. raw_readings (Postgres)         -- validated, deduplicated readings
  3. live_zone_metrics (Postgres)    -- 1-minute tumbling per-zone aggregates

See streaming/transforms.py for the DataFrame logic itself (unit tested in
isolation there) -- this module only wires that logic to the live Kafka source and
Postgres sinks, and handles logging/metrics per microbatch.
"""
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/app")
from common.logging_utils import get_logger, log_event
from common.metrics_utils import push_metrics
from streaming.postgres_io import upsert_raw_readings, upsert_zone_metrics
from streaming.transforms import (
    aggregate_zone_metrics,
    dedup_readings,
    enrich_readings,
    parse_readings,
    split_valid_invalid,
    to_dlq_payload,
)

logger = get_logger("spark-job")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SOURCE_TOPIC = "meter_readings"
DLQ_TOPIC = "meter_readings_dlq"
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "/tmp/spark-checkpoints")


def read_source_stream(spark: SparkSession):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def write_dlq_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    count = batch_df.count()
    (
        batch_df.write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", DLQ_TOPIC)
        .save()
    )
    log_event(logger, "invalid_readings_sent_to_dlq", batch_id=batch_id, count=count)
    push_metrics("spark_job_dlq", {"spark_dlq_messages_total": count})


def write_raw_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    rows = [
        (
            r.meter_id, r.household_id, r.grid_zone, r.power_consumption_kwh,
            r.solar_generation_kwh, r.net_kwh, r.is_exporting, r.event_timestamp,
        )
        for r in batch_df.select(
            "meter_id", "household_id", "grid_zone", "power_consumption_kwh",
            "solar_generation_kwh", "net_kwh", "is_exporting", "event_timestamp",
        ).collect()
    ]
    upsert_raw_readings(rows)
    log_event(logger, "raw_batch_processed", batch_id=batch_id, rows=len(rows))
    push_metrics("spark_job_raw", {"spark_raw_rows_processed_total": len(rows)})


def write_zone_metrics_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    rows = [
        (
            r.grid_zone, r.window_start, r.window_end, r.total_load_kwh,
            r.total_solar_kwh, r.solar_share_pct, r.exporting_households,
        )
        for r in batch_df.collect()
    ]
    upsert_zone_metrics(rows)
    log_event(logger, "zone_metrics_batch_processed", batch_id=batch_id, zones=len(rows))
    push_metrics("spark_job_zone_metrics", {"spark_zone_windows_updated_total": len(rows)})


def main():
    spark = (
        SparkSession.builder.appName("solarnet-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log_event(logger, "spark_job_starting", kafka=KAFKA_BOOTSTRAP)

    kafka_df = read_source_stream(spark)
    parsed_df = parse_readings(kafka_df)
    valid_df, invalid_df = split_valid_invalid(parsed_df)
    enriched_df = enrich_readings(valid_df)

    dlq_query = (
        to_dlq_payload(invalid_df)
        .writeStream.foreachBatch(write_dlq_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/dlq")
        .trigger(processingTime="10 seconds")
        .start()
    )

    raw_query = (
        dedup_readings(enriched_df)
        .writeStream.foreachBatch(write_raw_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw_readings")
        .trigger(processingTime="10 seconds")
        .start()
    )

    zone_query = (
        aggregate_zone_metrics(enriched_df)
        .writeStream.foreachBatch(write_zone_metrics_batch)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/zone_metrics")
        .trigger(processingTime="15 seconds")
        .start()
    )

    log_event(
        logger, "spark_job_streams_started",
        queries=[dlq_query.id, raw_query.id, zone_query.id],
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
