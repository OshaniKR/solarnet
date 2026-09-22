# solarnet
EC8203 Applied Big Data Engineering: Lambda architecture smart grid billing with solar.

## Processing layer (speed layer)

`streaming/spark_job.py` is a Spark Structured Streaming job that consumes the
`meter_readings` Kafka topic and fans out to three sinks:

1. **`meter_readings_dlq` (Kafka)** -- readings that fail validation (malformed JSON,
   missing fields, an unparseable timestamp, or a negative consumption/solar value),
   tagged with an `invalid_reason` and the original raw payload.
2. **`raw_readings` (Postgres)** -- validated readings, deduplicated with a
   watermark + `dropDuplicates` on `(meter_id, event_timestamp)`; the table's own
   UNIQUE constraint is the safety net for duplicates that fall outside that window.
3. **`live_zone_metrics` (Postgres)** -- 1-minute tumbling per-zone aggregates
   (total load, total solar, solar share %, exporting households), upserted on
   `(grid_zone, window_start)`.

The DataFrame transformations live in `streaming/transforms.py` and are unit tested
in isolation (no Kafka/Postgres needed) in `tests/test_streaming_transforms.py` --
`spark_job.py` itself only wires that logic to the live Kafka source and the
`streaming/postgres_io.py` upsert helpers, and handles per-microbatch logging/metrics.

Run the streaming tests:

```
pip install -r tests/requirements.txt
pytest tests/ -v
```

(Needs a JDK on your PATH -- pyspark starts a local JVM even for these
Kafka/Postgres-free unit tests.)
