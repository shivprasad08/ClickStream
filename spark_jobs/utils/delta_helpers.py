"""
delta_helpers.py
Shared helper functions for writing streaming DataFrames to Delta tables
and inspecting existing tables.  Used by Bronze, Silver, and Gold layers
to keep checkpoint / trigger conventions consistent.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery


def write_stream_to_delta(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    output_mode: str = "append",
    trigger_interval: str = "10 seconds",
    partition_by: str | None = None,
) -> StreamingQuery:
    """Start a streaming write to a Delta table.

    Parameters
    ----------
    df : DataFrame
        A *streaming* DataFrame to persist.
    output_path : str
        Filesystem path where the Delta table will be written.
    checkpoint_path : str
        Filesystem path for the streaming checkpoint (must be unique
        per query to guarantee exactly-once semantics).
    output_mode : str, default ``"append"``
        Spark output mode -- ``"append"`` for Bronze, ``"complete"``
        or ``"update"`` for Gold aggregations where applicable.
    trigger_interval : str, default ``"10 seconds"``
        Processing-time trigger interval.
    partition_by : str | None, default ``None``
        Optional column name to partition the Delta table by
        (e.g. ``"event_date"`` for Silver).

    Returns
    -------
    StreamingQuery
        The running query handle.  Caller decides whether to
        ``.awaitTermination()`` or manage it programmatically.
    """
    writer = (
        df.writeStream
        .format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
    )
    if partition_by:
        writer = writer.partitionBy(partition_by)
    return writer.start(output_path)


def table_exists(spark: SparkSession, path: str) -> bool:
    """Check whether a Delta table already exists at *path*.

    Used downstream (Silver/Gold) to decide between CREATE and MERGE
    logic.  Safe to call before any table has been written.
    """
    try:
        from delta.tables import DeltaTable

        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False
