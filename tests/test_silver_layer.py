"""
test_silver_layer.py
Unit tests for the Silver layer transformation logic.

Tests use an in-memory DataFrame that mimics the shape of Bronze output
(schema-enforced, with intentionally bad records).  The testable seam is
``process_silver_batch`` which applies the same casting / validation /
dedup / flattening logic as the streaming job.
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DateType,
)

# ── Make project packages importable ─────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs"))

from silver_layer import process_silver_batch


# ── Bronze-shaped schema (matches file_stream_reader.event_schema
#    + the two audit columns added by get_raw_batch / get_raw_stream) ──

BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("metadata", StructType([
        StructField("device_type", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("price", DoubleType(), True),
    ]), True),
    StructField("_corrupt_record", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("source_file", StringType(), True),
])


# ── Spark session ────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_silver_layer")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


# ── Test data ────────────────────────────────────────────────────────
# 6 rows total:
#   - 2 fully valid (event-A, event-B)
#   - 1 duplicate of event-A (same event_id)
#   - 1 with null user_id  (should be rejected)
#   - 1 with unparseable timestamp (should be rejected)
#   - 1 valid purchase with price (event-C)

TEST_ROWS = [
    # valid page_view
    Row(event_id="aaa-111", user_id=1, session_id="s1",
        event_type="page_view", product_id=10,
        timestamp="2026-07-04T10:00:00.000Z",
        metadata=Row(device_type="mobile", referrer="google", price=None),
        _corrupt_record=None, ingestion_timestamp=None, source_file="f1.json"),
    # valid search
    Row(event_id="bbb-222", user_id=2, session_id="s2",
        event_type="search", product_id=None,
        timestamp="2026-07-04T10:01:00.000Z",
        metadata=Row(device_type="desktop", referrer="direct", price=None),
        _corrupt_record=None, ingestion_timestamp=None, source_file="f2.json"),
    # DUPLICATE of event-A (same event_id)
    Row(event_id="aaa-111", user_id=1, session_id="s1",
        event_type="page_view", product_id=10,
        timestamp="2026-07-04T10:00:00.000Z",
        metadata=Row(device_type="mobile", referrer="google", price=None),
        _corrupt_record=None, ingestion_timestamp=None, source_file="f3.json"),
    # NULL user_id -- should be REJECTED
    Row(event_id="ccc-333", user_id=None, session_id="s3",
        event_type="add_to_cart", product_id=20,
        timestamp="2026-07-04T10:02:00.000Z",
        metadata=Row(device_type="tablet", referrer="email", price=None),
        _corrupt_record=None, ingestion_timestamp=None, source_file="f4.json"),
    # BAD timestamp -- should be REJECTED
    Row(event_id="ddd-444", user_id=3, session_id="s4",
        event_type="page_view", product_id=30,
        timestamp="NOT-A-REAL-TIMESTAMP",
        metadata=Row(device_type="mobile", referrer="facebook", price=None),
        _corrupt_record=None, ingestion_timestamp=None, source_file="f5.json"),
    # valid purchase with price
    Row(event_id="eee-555", user_id=4, session_id="s5",
        event_type="purchase", product_id=40,
        timestamp="2026-07-04T10:03:00.000Z",
        metadata=Row(device_type="desktop", referrer="instagram", price=149.99),
        _corrupt_record=None, ingestion_timestamp=None, source_file="f6.json"),
]


@pytest.fixture(scope="module")
def bronze_df(spark):
    return spark.createDataFrame(TEST_ROWS, schema=BRONZE_SCHEMA)


@pytest.fixture(scope="module")
def silver_dfs(bronze_df):
    return process_silver_batch(bronze_df)


# ======================================================================
#  Clean DataFrame tests
# ======================================================================


class TestCleanRecords:
    """Verify clean_df output from process_silver_batch."""

    def test_excludes_null_user_id(self, silver_dfs):
        clean_df, _ = silver_dfs
        null_uid = clean_df.filter(col("user_id").isNull()).count()
        assert null_uid == 0, "Null user_id records should be rejected"

    def test_excludes_bad_timestamp(self, silver_dfs):
        clean_df, _ = silver_dfs
        bad_ts = clean_df.filter(col("event_id") == "ddd-444").count()
        assert bad_ts == 0, "Bad-timestamp record should be rejected"

    def test_dedup_removes_duplicate(self, silver_dfs):
        """Only 1 copy of event_id='aaa-111' should survive."""
        clean_df, _ = silver_dfs
        dup_count = clean_df.filter(col("event_id") == "aaa-111").count()
        assert dup_count == 1, f"Expected 1 copy of aaa-111, got {dup_count}"

    def test_clean_row_count(self, silver_dfs):
        """3 unique valid events: aaa-111, bbb-222, eee-555."""
        clean_df, _ = silver_dfs
        assert clean_df.count() == 3, (
            f"Expected 3 clean rows, got {clean_df.count()}"
        )

    def test_event_date_derived(self, silver_dfs):
        """event_date should be a DateType column derived from timestamp."""
        clean_df, _ = silver_dfs
        assert clean_df.schema["event_date"].dataType == DateType()
        # All test events are on 2026-07-04
        dates = [r.event_date.isoformat() for r in clean_df.select("event_date").collect()]
        assert all(d == "2026-07-04" for d in dates)

    def test_event_timestamp_is_timestamp_type(self, silver_dfs):
        clean_df, _ = silver_dfs
        assert clean_df.schema["event_timestamp"].dataType == TimestampType()

    def test_metadata_flattened(self, silver_dfs):
        """device_type, referrer, price should be top-level columns."""
        clean_df, _ = silver_dfs
        cols = set(clean_df.columns)
        assert "device_type" in cols
        assert "referrer" in cols
        assert "price" in cols
        # The nested metadata struct should be dropped
        assert "metadata" not in cols

    def test_purchase_has_price(self, silver_dfs):
        clean_df, _ = silver_dfs
        purchase = clean_df.filter(col("event_id") == "eee-555").collect()[0]
        assert purchase.price == 149.99

    def test_non_purchase_no_price(self, silver_dfs):
        clean_df, _ = silver_dfs
        page_view = clean_df.filter(col("event_id") == "aaa-111").collect()[0]
        assert page_view.price is None

    def test_corrupt_record_column_dropped(self, silver_dfs):
        clean_df, _ = silver_dfs
        assert "_corrupt_record" not in clean_df.columns

    def test_raw_timestamp_column_dropped(self, silver_dfs):
        """Original StringType 'timestamp' should be replaced by event_timestamp."""
        clean_df, _ = silver_dfs
        assert "timestamp" not in clean_df.columns


# ======================================================================
#  Rejected DataFrame tests
# ======================================================================


class TestRejectedRecords:
    """Verify rejected_df output from process_silver_batch."""

    def test_rejected_count(self, silver_dfs):
        """Expect exactly 2 rejected: null user_id + bad timestamp."""
        _, rejected_df = silver_dfs
        assert rejected_df.count() == 2, (
            f"Expected 2 rejected rows, got {rejected_df.count()}"
        )

    def test_null_user_id_in_rejected(self, silver_dfs):
        _, rejected_df = silver_dfs
        null_uid = rejected_df.filter(col("event_id") == "ccc-333").count()
        assert null_uid == 1

    def test_bad_timestamp_in_rejected(self, silver_dfs):
        _, rejected_df = silver_dfs
        bad_ts = rejected_df.filter(col("event_id") == "ddd-444").count()
        assert bad_ts == 1

    def test_is_valid_false_on_rejected(self, silver_dfs):
        _, rejected_df = silver_dfs
        valid_count = rejected_df.filter(col("is_valid")).count()
        assert valid_count == 0, "All rejected records should have is_valid=False"


# ======================================================================
#  Total preservation test
# ======================================================================


class TestRowPreservation:
    """Clean + rejected + dedup removals should account for all input."""

    def test_total_equals_input_minus_dups(self, bronze_df, silver_dfs):
        """6 input - 1 dup = 5 unique; 3 clean + 2 rejected = 5."""
        clean_df, rejected_df = silver_dfs
        assert clean_df.count() + rejected_df.count() == 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
