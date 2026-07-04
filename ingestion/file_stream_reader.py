"""
file_stream_reader.py
Shared utility for Module 3 (bronze_layer.py) -- reads raw JSON events
from the file-based landing zone using Spark Structured Streaming.

This module is NOT a standalone service.  It exposes two pure functions:
  - get_raw_stream()        : sets up the streaming read
  - split_corrupt_records() : separates valid vs malformed rows

Design decisions:
  - No os.environ access here -- config (source_path, max_files_per_trigger)
    is passed in by the caller so the module stays purely functional.
  - No SparkSession creation -- that belongs in utils/spark_session.py.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Explicit schema for raw clickstream events ───────────────────────
#
# Spark Structured Streaming on file sources requires an explicit schema;
# it will NOT reliably infer schema on streaming reads (and inferring per
# micro-batch is expensive/inconsistent).
#
# NOTE: `timestamp` is intentionally kept as StringType at this layer.
# Bronze = raw fidelity -- we preserve the exact string the producer
# emitted (including intentionally bad/future/stale timestamps injected
# for testing).  Casting to TimestampType happens in the Silver layer
# where we also validate and reject out-of-range values.
#
# The `_corrupt_record` column is required by PERMISSIVE mode to capture
# rows where the JSON itself is structurally malformed (broken syntax,
# not the same as our intentionally-null-field events which are still
# valid JSON).

event_schema = StructType([
    StructField("event_id", StringType(), nullable=True),
    StructField("user_id", IntegerType(), nullable=True),
    StructField("session_id", StringType(), nullable=True),
    StructField("event_type", StringType(), nullable=True),
    StructField("product_id", IntegerType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True),
    StructField("metadata", StructType([
        StructField("device_type", StringType(), nullable=True),
        StructField("referrer", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
    ]), nullable=True),
    StructField("_corrupt_record", StringType(), nullable=True),
])


# ── Core streaming reader ────────────────────────────────────────────

def get_raw_stream(spark, source_path: str, max_files_per_trigger: int = 10):
    """Read the raw JSON event stream from *source_path*.

    Uses Spark Structured Streaming's file source with PERMISSIVE mode
    so structurally malformed JSON is captured in ``_corrupt_record``
    rather than crashing the stream or silently dropping rows.

    Parameters
    ----------
    spark : SparkSession
        An active SparkSession (created by the caller).
    source_path : str
        Directory being watched for new JSON files (``RAW_EVENTS_PATH``).
    max_files_per_trigger : int, default 10
        Caps how many new files are processed per micro-batch.
        Controls the latency vs throughput trade-off -- read from ``.env``
        in the calling code, passed in as a parameter here.

    Returns
    -------
    DataFrame
        A *streaming* DataFrame with the raw event schema plus two
        audit columns:

        * ``ingestion_timestamp`` -- when Spark saw the record (distinct
          from the event's own possibly-bad timestamp field).
        * ``source_file`` -- the input file the record came from, useful
          for debugging lineage.
    """
    raw_stream = (
        spark
        .readStream
        .format("json")
        .schema(event_schema)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(source_path)
    )

    enriched = (
        raw_stream
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    return enriched


# ── Batch reader (for testing / ad-hoc exploration) ──────────────────

def get_raw_batch(spark, source_path: str):
    """Non-streaming variant of :func:`get_raw_stream`.

    Reads the same schema in batch mode -- handy for unit tests and
    notebook exploration where a streaming context is not needed.

    The result is cached because Spark 4.x disallows querying a raw
    JSON source when the only referenced column is ``_corrupt_record``
    (UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN).  Caching
    materialises the data and lifts this restriction.
    """
    raw_df = (
        spark
        .read
        .format("json")
        .schema(event_schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(source_path)
    )

    enriched = (
        raw_df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    return enriched.cache()


# ── Quarantine helper ────────────────────────────────────────────────

def split_corrupt_records(df: DataFrame):
    """Split a raw DataFrame into ``(valid_df, corrupt_df)``.

    Bronze layer writes both:
    * valid records   -> ``bronze`` Delta table
    * corrupt records -> ``bronze_quarantine`` Delta table for later
      inspection / manual remediation.

    Parameters
    ----------
    df : DataFrame
        The output of :func:`get_raw_stream` or :func:`get_raw_batch`.

    Returns
    -------
    tuple[DataFrame, DataFrame]
        ``(valid_df, corrupt_df)``
    """
    from pyspark.sql.functions import col

    valid_df = df.filter(col("_corrupt_record").isNull())
    corrupt_df = df.filter(col("_corrupt_record").isNotNull())

    return valid_df, corrupt_df
