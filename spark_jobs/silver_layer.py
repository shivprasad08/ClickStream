"""
silver_layer.py
Curated, validated layer of the clickstream lakehouse pipeline.

Silver reads from the Bronze Delta table as a stream (using Delta's
native streaming-read capability -- NOT re-reading the raw JSON files),
applies cleaning, validation, deduplication, and light enrichment, then
writes a curated Silver Delta table partitioned by ``event_date``.

Transformations applied here (intentionally skipped by Bronze):

  1. **Timestamp casting** -- ``timestamp`` StringType -> TimestampType.
     Records with unparseable timestamps are flagged and rejected.
  2. **Null-field validation** -- ``user_id IS NULL`` records (from
     Module 1's inject_missing_field) are flagged and rejected.
  3. **Watermarking** -- 10-minute watermark on ``event_timestamp``
     to bound state for stateful operations.  Events arriving more
     than 10 minutes late (relative to max timestamp seen so far)
     will be dropped from stateful ops.  This is an acceptable
     tradeoff for a streaming pipeline; documented as a known
     limitation in the README.
  4. **Deduplication** -- removes duplicate ``event_id`` records
     (from Module 1's inject_duplicate) within the watermark window.
  5. **Metadata flattening** -- ``metadata.device_type``,
     ``metadata.referrer``, and ``metadata.price`` are promoted to
     top-level columns for easier downstream querying.

Rejected records are written to ``silver/rejected`` as an append-only
data-quality audit trail.

Usage (Docker)::

    spark-submit --packages io.delta:delta-spark_2.12:3.2.1 silver_layer.py
"""

import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    to_date,
    to_timestamp,
    when,
)

# ── Make sibling packages importable ─────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.spark_session import get_spark_session
from utils.delta_helpers import write_stream_to_delta

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("silver-layer")

# ── Configuration ────────────────────────────────────────────────────
load_dotenv()

STORAGE_PATH: str = os.getenv("STORAGE_PATH", "./storage")

BRONZE_EVENTS_PATH: str = os.path.join(STORAGE_PATH, "bronze", "events")
SILVER_EVENTS_PATH: str = os.path.join(STORAGE_PATH, "silver", "events")
SILVER_REJECTED_PATH: str = os.path.join(STORAGE_PATH, "silver", "rejected")
CHECKPOINT_SILVER: str = os.path.join(
    STORAGE_PATH, "checkpoints", "silver_events"
)
CHECKPOINT_REJECTED: str = os.path.join(
    STORAGE_PATH, "checkpoints", "silver_rejected"
)

WATERMARK_DELAY: str = "10 minutes"


# ======================================================================
#  Batch-testable transformation logic
# ======================================================================


def process_silver_batch(bronze_df: DataFrame):
    """Apply Silver-layer transformations to a Bronze DataFrame.

    This function is the shared seam used by both the streaming job
    and unit tests.  It performs:

    1. Timestamp casting + validation flags
    2. Null user_id validation flag
    3. Split into clean / rejected
    4. Deduplication on ``event_id`` (batch: ``dropDuplicates``;
       streaming callers add watermark + ``dropDuplicatesWithinWatermark``
       before calling the writer)
    5. Metadata flattening + ``event_date`` derivation

    Parameters
    ----------
    bronze_df : DataFrame
        A Bronze-shaped DataFrame (batch or streaming).

    Returns
    -------
    tuple[DataFrame, DataFrame]
        ``(clean_df, rejected_df)``
    """
    # ── 1. Cast timestamp ────────────────────────────────────────
    with_ts = bronze_df.withColumn(
        "event_timestamp", to_timestamp(col("timestamp"))
    )

    # ── 2. Validation flags ──────────────────────────────────────
    validated = (
        with_ts
        .withColumn(
            "is_valid_timestamp",
            # If original timestamp was non-null but casting returned
            # null, the value was unparseable.
            when(
                col("timestamp").isNotNull() & col("event_timestamp").isNull(),
                False,
            ).otherwise(
                col("timestamp").isNotNull()  # null original -> invalid
            ),
        )
        .withColumn(
            "is_valid_user_id",
            col("user_id").isNotNull(),
        )
        .withColumn(
            "is_valid",
            col("is_valid_timestamp") & col("is_valid_user_id"),
        )
    )

    # ── 3. Split ─────────────────────────────────────────────────
    rejected_df = validated.filter(~col("is_valid"))
    clean_df = validated.filter(col("is_valid"))

    # ── 4. Deduplicate (batch-safe; streaming uses watermark) ────
    clean_df = clean_df.dropDuplicates(["event_id"])

    # ── 5. Flatten metadata + derive event_date ──────────────────
    clean_df = (
        clean_df
        .withColumn("device_type", col("metadata.device_type"))
        .withColumn("referrer", col("metadata.referrer"))
        .withColumn("price", col("metadata.price"))
        .withColumn("event_date", to_date(col("event_timestamp")))
        .drop("metadata", "timestamp", "_corrupt_record",
               "is_valid_timestamp", "is_valid_user_id", "is_valid")
    )

    return clean_df, rejected_df


# ======================================================================
#  Streaming-specific helpers
# ======================================================================


def _apply_watermark_and_dedup(clean_stream: DataFrame) -> DataFrame:
    """Add watermark and streaming-aware dedup to the clean stream.

    The 10-minute watermark tells Spark how long to keep dedup state:
    events arriving more than 10 minutes late (relative to the max
    ``event_timestamp`` seen so far) will be dropped from stateful
    operations.

    ``dropDuplicatesWithinWatermark`` (Spark 3.5+) is used because it
    correctly scopes dedup to the watermark window, unlike bare
    ``dropDuplicates`` which would grow state unboundedly in streaming.
    """
    return (
        clean_stream
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark(["event_id"])
    )


def _make_batch_logger(stream_name: str):
    """Return a ``foreachBatch`` callback that logs batch stats."""

    def _log_batch(batch_df, batch_id):
        count = batch_df.count()
        log.info("[%s] batch_id=%d  rows=%d", stream_name, batch_id, count)
        if count > 0:
            if stream_name == "silver_events":
                batch_df.write.format("delta").mode("append") \
                    .partitionBy("event_date") \
                    .save(SILVER_EVENTS_PATH)
            else:
                batch_df.write.format("delta").mode("append") \
                    .save(SILVER_REJECTED_PATH)

    return _log_batch


# ======================================================================
#  Main
# ======================================================================


def main() -> None:
    log.info("=" * 60)
    log.info("Silver Layer starting")
    log.info("  BRONZE_EVENTS_PATH  = %s", BRONZE_EVENTS_PATH)
    log.info("  SILVER_EVENTS_PATH  = %s", SILVER_EVENTS_PATH)
    log.info("  SILVER_REJECTED     = %s", SILVER_REJECTED_PATH)
    log.info("  WATERMARK_DELAY     = %s", WATERMARK_DELAY)
    log.info("=" * 60)

    # 1. SparkSession
    spark = get_spark_session("silver-layer")

    # 2. Read Bronze Delta table as a stream
    #    This is why Bronze had to be a Delta table and not just Parquet:
    #    Delta supports streaming reads on a table that is itself being
    #    written to by another streaming query (Bronze's writer), which
    #    lets Silver run continuously as new Bronze data lands.
    bronze_stream = (
        spark.readStream
        .format("delta")
        .load(BRONZE_EVENTS_PATH)
    )

    # 3. Apply Silver transformations (cast, validate, flatten)
    #    We pass the streaming DF through the same logic as batch tests.
    #    Timestamp casting + validation + split happen inside.
    with_ts = bronze_stream.withColumn(
        "event_timestamp", to_timestamp(col("timestamp"))
    )

    validated = (
        with_ts
        .withColumn(
            "is_valid_timestamp",
            when(
                col("timestamp").isNotNull() & col("event_timestamp").isNull(),
                False,
            ).otherwise(col("timestamp").isNotNull()),
        )
        .withColumn("is_valid_user_id", col("user_id").isNotNull())
        .withColumn(
            "is_valid",
            col("is_valid_timestamp") & col("is_valid_user_id"),
        )
    )

    rejected_df = validated.filter(~col("is_valid"))
    clean_df = validated.filter(col("is_valid"))

    # 4. Watermark + dedup (streaming-specific)
    clean_df = _apply_watermark_and_dedup(clean_df)

    # 5. Flatten metadata + derive event_date
    clean_df = (
        clean_df
        .withColumn("device_type", col("metadata.device_type"))
        .withColumn("referrer", col("metadata.referrer"))
        .withColumn("price", col("metadata.price"))
        .withColumn("event_date", to_date(col("event_timestamp")))
        .drop("metadata", "timestamp", "_corrupt_record",
               "is_valid_timestamp", "is_valid_user_id", "is_valid")
    )

    # 6. Start streaming writes
    log.info("Starting silver_events streaming query ...")
    clean_query = (
        clean_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_SILVER)
        .trigger(processingTime="15 seconds")
        .foreachBatch(_make_batch_logger("silver_events"))
        .start()
    )

    log.info("Starting silver_rejected streaming query ...")
    rejected_query = (
        rejected_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_REJECTED)
        .trigger(processingTime="15 seconds")
        .foreachBatch(_make_batch_logger("silver_rejected"))
        .start()
    )

    log.info("Both streaming queries running. Ctrl+C to stop.")

    # 7. Block until either query terminates
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
