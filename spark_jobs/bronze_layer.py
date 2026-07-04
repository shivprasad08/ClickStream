"""
bronze_layer.py
Raw, immutable landing layer for the clickstream lakehouse pipeline.

Bronze persists *exactly* what Module 2's ``file_stream_reader`` produced:
schema-enforced but NOT cleaned or validated.  Specifically, Bronze does
**not**:

  - Deduplicate records (duplicate injection from the producer is
    intentional; dedup is Silver's job).
  - Cast ``timestamp`` from StringType to TimestampType (raw fidelity;
    casting + watermark validation is Silver's job).
  - Drop nulls / reject missing-field records (again, Silver).

This is by design: Bronze is the audit-grade, append-only copy of
everything the producer emitted, warts and all.  Corrupt records
(structurally malformed JSON) are quarantined into a separate Delta
table for inspection.

Usage (Docker)::

    spark-submit --packages io.delta:delta-spark_2.12:3.2.1 bronze_layer.py

Usage (local dev)::

    python bronze_layer.py  # requires delta-spark in Python env
"""

import logging
import os
import sys

from dotenv import load_dotenv

# ── Add parent paths so sibling packages are importable ──────────────
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

from utils.spark_session import get_spark_session
from utils.delta_helpers import write_stream_to_delta
from ingestion.file_stream_reader import get_raw_stream, split_corrupt_records

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("bronze-layer")

# ── Configuration ────────────────────────────────────────────────────
load_dotenv()

RAW_EVENTS_PATH: str = os.getenv("RAW_EVENTS_PATH", "./data/raw_events")
STORAGE_PATH: str = os.getenv("STORAGE_PATH", "./storage")
MAX_FILES_PER_TRIGGER: int = int(os.getenv("MAX_FILES_PER_TRIGGER", "10"))

BRONZE_EVENTS_PATH: str = os.path.join(STORAGE_PATH, "bronze", "events")
BRONZE_QUARANTINE_PATH: str = os.path.join(STORAGE_PATH, "bronze", "quarantine")
CHECKPOINT_EVENTS: str = os.path.join(STORAGE_PATH, "checkpoints", "bronze_events")
CHECKPOINT_QUARANTINE: str = os.path.join(
    STORAGE_PATH, "checkpoints", "bronze_quarantine"
)


# ══════════════════════════════════════════════════════════════════════
#  Batch-testable transformation logic
# ══════════════════════════════════════════════════════════════════════


def process_bronze(raw_df):
    """Split a raw DataFrame into valid and corrupt partitions.

    This function is intentionally thin -- Bronze does not transform
    data.  It exists as a seam so both the streaming job and unit
    tests can call the same code path.

    Parameters
    ----------
    raw_df : DataFrame
        Output of ``get_raw_stream`` or ``get_raw_batch``.

    Returns
    -------
    tuple[DataFrame, DataFrame]
        ``(valid_df, corrupt_df)``
    """
    return split_corrupt_records(raw_df)


# ══════════════════════════════════════════════════════════════════════
#  Micro-batch logging via foreachBatch
# ══════════════════════════════════════════════════════════════════════


def _make_batch_logger(stream_name: str):
    """Return a ``foreachBatch`` callback that logs batch stats."""

    def _log_batch(batch_df, batch_id):
        count = batch_df.count()
        log.info("[%s] batch_id=%d  rows=%d", stream_name, batch_id, count)
        # Write to Delta inside the foreachBatch callback
        if count > 0:
            batch_df.write.format("delta").mode("append").save(
                BRONZE_EVENTS_PATH if stream_name == "bronze_events"
                else BRONZE_QUARANTINE_PATH
            )

    return _log_batch


# ══════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════


def main() -> None:
    log.info("=" * 60)
    log.info("Bronze Layer starting")
    log.info("  RAW_EVENTS_PATH      = %s", RAW_EVENTS_PATH)
    log.info("  STORAGE_PATH         = %s", STORAGE_PATH)
    log.info("  MAX_FILES_PER_TRIGGER = %d", MAX_FILES_PER_TRIGGER)
    log.info("  BRONZE_EVENTS_PATH   = %s", BRONZE_EVENTS_PATH)
    log.info("  BRONZE_QUARANTINE    = %s", BRONZE_QUARANTINE_PATH)
    log.info("=" * 60)

    # 1. Build SparkSession
    spark = get_spark_session("bronze-layer")

    # 2. Read raw stream from the file landing zone
    raw_df = get_raw_stream(spark, RAW_EVENTS_PATH, MAX_FILES_PER_TRIGGER)

    # 3. Split valid vs corrupt records
    valid_df, corrupt_df = process_bronze(raw_df)

    # 4. Start streaming writes -- two independent queries
    #    Both use foreachBatch for per-batch logging.
    log.info("Starting bronze_events streaming query ...")
    valid_query = (
        valid_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_EVENTS)
        .trigger(processingTime="10 seconds")
        .foreachBatch(_make_batch_logger("bronze_events"))
        .start()
    )

    log.info("Starting bronze_quarantine streaming query ...")
    quarantine_query = (
        corrupt_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_QUARANTINE)
        .trigger(processingTime="10 seconds")
        .foreachBatch(_make_batch_logger("bronze_quarantine"))
        .start()
    )

    log.info("Both streaming queries running. Ctrl+C to stop.")

    # 5. Block until either query terminates (or user sends SIGINT)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
