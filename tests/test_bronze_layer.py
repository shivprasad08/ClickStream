"""
test_bronze_layer.py
Unit tests for the Bronze layer transformation logic.

Tests use batch mode against the static fixture files from Module 2
(tests/fixtures/sample_events/) rather than a live streaming context.
The ``process_bronze`` function is the testable seam extracted from
bronze_layer.py -- it applies the same split logic that the streaming
job uses.
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ── Make project packages importable ─────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

from file_stream_reader import get_raw_batch
from bronze_layer import process_bronze

# ── Fixtures path ────────────────────────────────────────────────────
FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "sample_events")


# ── Spark session fixture ────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_bronze_layer")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def raw_df(spark):
    """Read fixture files once via the batch reader."""
    return get_raw_batch(spark, FIXTURES_DIR)


@pytest.fixture(scope="module")
def bronze_dfs(raw_df):
    """Run the Bronze transformation (split valid / corrupt)."""
    return process_bronze(raw_df)


# ══════════════════════════════════════════════════════════════════════
#  Valid-record tests
# ══════════════════════════════════════════════════════════════════════


class TestValidRecords:
    """Verify valid_df from process_bronze."""

    def test_valid_row_count(self, bronze_dfs):
        """Expect 4 valid rows: 3 clean + 1 missing-field (still valid JSON)."""
        valid_df, _ = bronze_dfs
        assert valid_df.count() == 4, (
            f"Expected 4 valid rows, got {valid_df.count()}"
        )

    def test_valid_df_has_core_columns(self, bronze_dfs):
        valid_df, _ = bronze_dfs
        expected = {
            "event_id", "user_id", "session_id", "event_type",
            "product_id", "timestamp", "metadata",
        }
        assert expected.issubset(set(valid_df.columns))

    def test_ingestion_timestamp_populated(self, bronze_dfs):
        valid_df, _ = bronze_dfs
        null_count = valid_df.filter(col("ingestion_timestamp").isNull()).count()
        assert null_count == 0, "ingestion_timestamp should never be null"

    def test_source_file_populated(self, bronze_dfs):
        valid_df, _ = bronze_dfs
        null_count = valid_df.filter(col("source_file").isNull()).count()
        assert null_count == 0, "source_file should never be null"

    def test_no_corrupt_values_in_valid_df(self, bronze_dfs):
        valid_df, _ = bronze_dfs
        bad = valid_df.filter(col("_corrupt_record").isNotNull()).count()
        assert bad == 0

    def test_missing_field_event_preserved(self, bronze_dfs):
        """Null user_id/timestamp is valid JSON -- should be in valid_df."""
        valid_df, _ = bronze_dfs
        null_uid = valid_df.filter(col("user_id").isNull()).count()
        assert null_uid >= 1, "Missing-field fixture should be in valid_df"


# ══════════════════════════════════════════════════════════════════════
#  Corrupt-record tests
# ══════════════════════════════════════════════════════════════════════


class TestCorruptRecords:
    """Verify corrupt_df from process_bronze."""

    def test_corrupt_row_count(self, bronze_dfs):
        """The malformed fixture should land in corrupt_df."""
        _, corrupt_df = bronze_dfs
        assert corrupt_df.count() >= 1, "Expected at least 1 corrupt record"

    def test_corrupt_record_column_populated(self, bronze_dfs):
        _, corrupt_df = bronze_dfs
        null_count = corrupt_df.filter(col("_corrupt_record").isNull()).count()
        assert null_count == 0, (
            "Every row in corrupt_df must have _corrupt_record populated"
        )

    def test_total_rows_match_raw(self, raw_df, bronze_dfs):
        valid_df, corrupt_df = bronze_dfs
        assert valid_df.count() + corrupt_df.count() == raw_df.count()


# ══════════════════════════════════════════════════════════════════════
#  Raw-fidelity contract tests
# ══════════════════════════════════════════════════════════════════════


class TestRawFidelity:
    """Bronze must NOT transform data -- verify raw pass-through."""

    def test_timestamp_stays_string(self, bronze_dfs):
        """timestamp must remain StringType (no casting at Bronze)."""
        from pyspark.sql.types import StringType

        valid_df, _ = bronze_dfs
        assert valid_df.schema["timestamp"].dataType == StringType()

    def test_no_dedup(self, raw_df, bronze_dfs):
        """Row count should equal raw input -- no dedup applied."""
        valid_df, corrupt_df = bronze_dfs
        assert valid_df.count() + corrupt_df.count() == raw_df.count()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
