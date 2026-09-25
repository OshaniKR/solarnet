"""
Pure Spark DataFrame transformations for the SolarNet streaming job.

Kept separate from spark_job.py's I/O wiring (Kafka source, Postgres sinks) so these
can be unit tested against a small local DataFrame without a running Kafka or
Postgres -- see tests/test_streaming_transforms.py. window()/withWatermark() behave
the same on a static DataFrame as on a streaming one (the watermark is just inert
metadata off a live stream), which is what makes that possible.
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"

READING_SCHEMA = StructType([
    StructField("meter_id", StringType()),
    StructField("household_id", StringType()),
    StructField("grid_zone", StringType()),
    StructField("power_consumption_kwh", DoubleType()),
    StructField("solar_generation_kwh", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("_corrupt_record", StringType()),
])


def parse_readings(kafka_df: DataFrame) -> DataFrame:
    """kafka_df needs (at least) `key` and `value` columns, as produced by the Kafka
    source (bytes) or a plain string test DataFrame. Returns one row per input
    message with the reading fields flattened alongside the original key/raw value,
    and `event_timestamp` parsed to a real TimestampType."""
    parsed = kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.from_json(
            F.col("value").cast("string"),
            READING_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ).alias("data"),
    )
    return parsed.select("kafka_key", "raw_value", "data.*").withColumn(
        "event_timestamp", F.to_timestamp(F.col("timestamp"), TIMESTAMP_FORMAT)
    )


def _invalid_reason(df: DataFrame):
    return (
        F.when(F.col("_corrupt_record").isNotNull(), F.lit("malformed_json"))
        .when(
            F.col("meter_id").isNull()
            | F.col("household_id").isNull()
            | F.col("grid_zone").isNull(),
            F.lit("missing_required_field"),
        )
        .when(F.col("event_timestamp").isNull(), F.lit("unparseable_timestamp"))
        .when(
            F.col("power_consumption_kwh").isNull() | (F.col("power_consumption_kwh") < 0),
            F.lit("invalid_consumption_value"),
        )
        .when(
            F.col("solar_generation_kwh").isNull() | (F.col("solar_generation_kwh") < 0),
            F.lit("invalid_solar_value"),
        )
        .otherwise(F.lit(None).cast("string"))
    )


def split_valid_invalid(parsed_df: DataFrame):
    """Returns (valid_df, invalid_df). invalid_df carries an `invalid_reason` column
    explaining the rejection, for the DLQ payload."""
    reasoned = parsed_df.withColumn("invalid_reason", _invalid_reason(parsed_df))
    valid_df = reasoned.filter(F.col("invalid_reason").isNull()).drop(
        "invalid_reason", "_corrupt_record"
    )
    invalid_df = reasoned.filter(F.col("invalid_reason").isNotNull())
    return valid_df, invalid_df


def enrich_readings(valid_df: DataFrame) -> DataFrame:
    """Adds the derived net-billing columns used by both the raw_readings sink and
    the zone-metrics aggregation. net_kwh < 0 means the household sent more solar to
    the grid than it drew -- see common/billing.py for how this later becomes a bill."""
    return valid_df.withColumn(
        "net_kwh", F.round(F.col("power_consumption_kwh") - F.col("solar_generation_kwh"), 4)
    ).withColumn("is_exporting", F.col("net_kwh") < 0)


def dedup_readings(enriched_df: DataFrame, watermark: str = "5 minutes") -> DataFrame:
    """Streaming-only concern: bounds state size with a watermark on event_timestamp,
    then drops exact duplicate (meter_id, event_timestamp) pairs within that window.
    raw_readings' UNIQUE constraint is the safety net for duplicates that arrive
    further apart than the watermark allows (see sql/schema.sql)."""
    return enriched_df.withWatermark("event_timestamp", watermark).dropDuplicates(
        ["meter_id", "event_timestamp"]
    )


def to_dlq_payload(invalid_df: DataFrame) -> DataFrame:
    """Shapes rejected rows into Kafka-sink-ready (key, value) columns for the
    `meter_readings_dlq` topic: the original raw payload plus why it was rejected."""
    return invalid_df.select(
        F.col("kafka_key").alias("key"),
        F.to_json(
            F.struct(
                F.col("raw_value"),
                F.col("invalid_reason"),
                F.current_timestamp().alias("rejected_at"),
            )
        ).alias("value"),
    )


def aggregate_zone_metrics(
    enriched_df: DataFrame, window: str = "1 minute", watermark: str = "5 minutes"
) -> DataFrame:
    """Tumbling-window per-zone aggregation feeding live_zone_metrics. solar_share_pct
    is total solar generation as a percentage of total load in that zone/window --
    what the API's LOW_SOLAR_SHARE_PCT alert watches during daylight hours."""
    windowed = (
        enriched_df.withWatermark("event_timestamp", watermark)
        .groupBy(F.window(F.col("event_timestamp"), window).alias("w"), F.col("grid_zone"))
        .agg(
            F.sum("power_consumption_kwh").alias("total_load_kwh"),
            F.sum("solar_generation_kwh").alias("total_solar_kwh"),
            # Spark rejects exact distinct aggregations on streaming DataFrames;
            # HyperLogLog++ is effectively exact at this cardinality (tens of households).
            F.approx_count_distinct(
                F.when(F.col("is_exporting"), F.col("household_id")), rsd=0.01
            ).cast("int").alias("exporting_households"),
        )
    )
    return windowed.select(
        F.col("grid_zone"),
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        F.round(F.col("total_load_kwh"), 4).alias("total_load_kwh"),
        F.round(F.col("total_solar_kwh"), 4).alias("total_solar_kwh"),
        F.when(
            F.col("total_load_kwh") > 0,
            F.round(F.col("total_solar_kwh") / F.col("total_load_kwh") * 100, 2),
        ).otherwise(F.lit(0.0)).alias("solar_share_pct"),
        F.col("exporting_households"),
    )
