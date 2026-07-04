"""
gold_layer.py
Business-level aggregation layer of the clickstream lakehouse pipeline.

Gold reads from the curated Silver Delta table as a stream, computes
windowed business metrics, and writes the results to Delta tables.

Transformations applied here:
  1. **Event Counts**: 5-minute tumbling window counts per event type.
     Written in `append` mode since windows are finalized after the watermark.
  2. **Session Metrics**: Session-level aggregation (start/end times,
     event count, total revenue). Because events for a session can arrive
     out of order or late, this requires `update` output mode and Delta's
     MERGE INTO for CDC-style upserts.

Usage (Docker)::

    spark-submit --packages io.delta:delta-spark_2.12:3.2.1 gold_layer.py
"""

import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    max as _max,
    min as _min,
    sum as _sum,
    when,
    window,
)

# ── Make sibling packages importable ─────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.spark_session import get_spark_session
from utils.delta_helpers import write_stream_to_delta, table_exists

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("gold-layer")

# ── Configuration ────────────────────────────────────────────────────
load_dotenv()

STORAGE_PATH: str = os.getenv("STORAGE_PATH", "./storage")

SILVER_EVENTS_PATH: str = os.path.join(STORAGE_PATH, "silver", "events")
GOLD_EVENT_COUNTS_PATH: str = os.path.join(STORAGE_PATH, "gold", "event_counts")
GOLD_SESSION_METRICS_PATH: str = os.path.join(
    STORAGE_PATH, "gold", "session_metrics"
)
CHECKPOINT_COUNTS: str = os.path.join(
    STORAGE_PATH, "checkpoints", "gold_event_counts"
)
CHECKPOINT_SESSIONS: str = os.path.join(
    STORAGE_PATH, "checkpoints", "gold_session_metrics"
)

WATERMARK_DELAY: str = "10 minutes"


# ======================================================================
#  Batch-testable transformation logic
# ======================================================================


def compute_event_counts(silver_df: DataFrame) -> DataFrame:
    """Compute rolling 5-minute event counts per event_type.

    Parameters
    ----------
    silver_df : DataFrame
        A Silver-shaped DataFrame (batch or streaming) containing
        ``event_timestamp`` and ``event_type``.

    Returns
    -------
    DataFrame
        Aggregated counts with ``window_start``, ``window_end``,
        ``event_type``, and ``event_count``.
    """
    event_counts = (
        silver_df
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("event_type")
        )
        .agg(count("*").alias("event_count"))
    )

    # Flatten the window struct for easier downstream querying
    flattened = (
        event_counts
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )

    return flattened


def compute_session_metrics(silver_df: DataFrame) -> DataFrame:
    """Compute session-level aggregations (start, end, count, revenue).

    Parameters
    ----------
    silver_df : DataFrame
        A Silver-shaped DataFrame containing ``session_id``, ``user_id``,
        ``event_timestamp``, ``event_type``, and ``price``.

    Returns
    -------
    DataFrame
        Aggregated metrics per session.
    """
    session_metrics = (
        silver_df
        .groupBy("session_id", "user_id")
        .agg(
            _min("event_timestamp").alias("session_start"),
            _max("event_timestamp").alias("session_end"),
            count("*").alias("event_count_in_session"),
            _sum(
                when(col("event_type") == "purchase", col("price"))
                .otherwise(0)
            ).alias("session_revenue"),
        )
    )

    return session_metrics


# ======================================================================
#  Streaming helpers & CDC Upsert
# ======================================================================


def ensure_gold_table_exists(spark, path: str, schema):
    """Bootstrap an empty Delta table if it doesn't exist yet.

    DeltaTable.forPath() will fail if the table hasn't been created,
    so we need this for the very first MERGE INTO operation.
    """
    if not table_exists(spark, path):
        log.info("Bootstrapping target Delta table at %s", path)
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").save(path)


def make_upsert_session_metrics(spark, target_path: str):
    """Return a foreachBatch function that performs MERGE INTO."""

    def upsert_batch(microbatch_df, batch_id):
        log.info("[session_metrics] Upserting batch_id=%d", batch_id)
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(spark, target_path)

        (
            delta_table.alias("target")
            .merge(
                microbatch_df.alias("source"),
                "target.session_id = source.session_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    return upsert_batch


# ======================================================================
#  Main
# ======================================================================


def main() -> None:
    log.info("=" * 60)
    log.info("Gold Layer starting")
    log.info("  SILVER_EVENTS_PATH        = %s", SILVER_EVENTS_PATH)
    log.info("  GOLD_EVENT_COUNTS_PATH    = %s", GOLD_EVENT_COUNTS_PATH)
    log.info("  GOLD_SESSION_METRICS_PATH = %s", GOLD_SESSION_METRICS_PATH)
    log.info("=" * 60)

    # 1. SparkSession
    spark = get_spark_session("gold-layer")

    # 2. Read Silver Delta table as a stream
    silver_stream = (
        spark.readStream
        .format("delta")
        .load(SILVER_EVENTS_PATH)
    )

    # Note: watermarks do not carry over from the Silver pipeline,
    # we must re-apply them on the stream here so Spark knows how to
    # bound state for Gold aggregations.
    watermarked_stream = silver_stream.withWatermark(
        "event_timestamp", WATERMARK_DELAY
    )

    # 3. Compute Metrics
    event_counts = compute_event_counts(watermarked_stream)
    session_metrics = compute_session_metrics(watermarked_stream)

    # 4. Bootstrap target table for session metrics
    # We need the schema to bootstrap. We can derive it by transforming
    # a limit(0) batch from the silver path.
    if not table_exists(spark, GOLD_SESSION_METRICS_PATH):
        try:
            silver_batch = spark.read.format("delta").load(SILVER_EVENTS_PATH).limit(0)
            session_schema = compute_session_metrics(silver_batch).schema
            ensure_gold_table_exists(spark, GOLD_SESSION_METRICS_PATH, session_schema)
        except Exception as e:
            log.warning("Could not bootstrap session metrics table (Silver might be empty): %s", e)
            # It's okay if Silver is totally empty; if we fail here, the first
            # microbatch containing data will crash on MERGE, but since we retry,
            # we'll bootstrap it later or we just wait. Actually, it's safer to
            # explicitly define the schema to avoid race conditions.
            from pyspark.sql.types import (
                StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
            )
            fallback_schema = StructType([
                StructField("session_id", StringType(), True),
                StructField("user_id", IntegerType(), True),
                StructField("session_start", TimestampType(), True),
                StructField("session_end", TimestampType(), True),
                StructField("event_count_in_session", IntegerType(), False),
                StructField("session_revenue", DoubleType(), True),
            ])
            ensure_gold_table_exists(spark, GOLD_SESSION_METRICS_PATH, fallback_schema)


    # 5. Start streaming writes
    log.info("Starting gold_event_counts streaming query ...")
    # Event counts are safe for append-only (once watermark passes window_end)
    counts_query = (
        event_counts.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_COUNTS)
        .trigger(processingTime="15 seconds")
        .start(GOLD_EVENT_COUNTS_PATH)
    )

    log.info("Starting gold_session_metrics streaming query ...")
    # Session metrics must be update mode for foreachBatch MERGE
    session_query = (
        session_metrics.writeStream
        .outputMode("update")
        .foreachBatch(make_upsert_session_metrics(spark, GOLD_SESSION_METRICS_PATH))
        .option("checkpointLocation", CHECKPOINT_SESSIONS)
        .trigger(processingTime="15 seconds")
        .start()
    )

    log.info("Both streaming queries running. Ctrl+C to stop.")

    # 6. Block until either query terminates
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
