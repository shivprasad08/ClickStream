"""
__init__.py
Utils package for Spark Structured Streaming and Delta Lake.

Re-exports the two main helpers so callers can write::

    from utils.spark_session import get_spark_session
    from utils.delta_helpers import write_stream_to_delta, table_exists
"""
