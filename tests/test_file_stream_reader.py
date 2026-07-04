"""
test_file_stream_reader.py
Unit tests for the ingestion layer (file_stream_reader.py).

Uses a static batch read against fixture JSON files rather than a live
streaming context, since full streaming verification happens in Module 3
once bronze_layer.py wires it together with an actual running query.
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType

# ── Make ingestion/ importable ───────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

from file_stream_reader import event_schema, get_raw_batch, split_corrupt_records

# ── Fixtures path ────────────────────────────────────────────────────
FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "sample_events")


# ── Spark session fixture (shared across module) ─────────────────────

@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_file_stream_reader")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def raw_df(spark):
    """Read the fixture files once via get_raw_batch."""
    return get_raw_batch(spark, FIXTURES_DIR)


# ══════════════════════════════════════════════════════════════════════
#  Schema tests
# ══════════════════════════════════════════════════════════════════════


class TestSchema:
    """Verify the schema returned by get_raw_batch matches expectations."""

    def test_schema_has_core_event_fields(self, raw_df):
        field_names = set(raw_df.columns)
        expected = {
            "event_id", "user_id", "session_id", "event_type",
            "product_id", "timestamp", "metadata", "_corrupt_record",
        }
        assert expected.issubset(field_names), (
            f"Missing columns: {expected - field_names}"
        )

    def test_schema_has_audit_columns(self, raw_df):
        field_names = set(raw_df.columns)
        assert "ingestion_timestamp" in field_names
        assert "source_file" in field_names

    def test_event_id_is_string_type(self, raw_df):
        assert raw_df.schema["event_id"].dataType == StringType()

    def test_user_id_is_integer_type(self, raw_df):
        assert raw_df.schema["user_id"].dataType == IntegerType()

    def test_timestamp_is_string_type(self, raw_df):
        """Bronze preserves raw fidelity -- timestamp stays StringType."""
        assert raw_df.schema["timestamp"].dataType == StringType()

    def test_metadata_nested_schema(self, raw_df):
        meta_type = raw_df.schema["metadata"].dataType
        assert isinstance(meta_type, StructType)
        meta_fields = {f.name for f in meta_type.fields}
        assert {"device_type", "referrer", "price"}.issubset(meta_fields)

    def test_corrupt_record_is_string_type(self, raw_df):
        assert raw_df.schema["_corrupt_record"].dataType == StringType()


# ══════════════════════════════════════════════════════════════════════
#  Corrupt-record splitting tests
# ══════════════════════════════════════════════════════════════════════


class TestSplitCorruptRecords:
    """Verify split_corrupt_records correctly separates valid vs corrupt."""

    def test_total_rows_match(self, raw_df):
        """valid + corrupt should equal total rows read."""
        valid_df, corrupt_df = split_corrupt_records(raw_df)
        total = raw_df.count()
        assert valid_df.count() + corrupt_df.count() == total

    def test_valid_df_has_no_corrupt_column_values(self, raw_df):
        valid_df, _ = split_corrupt_records(raw_df)
        # All _corrupt_record values should be null in valid_df
        from pyspark.sql.functions import col
        non_null = valid_df.filter(col("_corrupt_record").isNotNull()).count()
        assert non_null == 0

    def test_corrupt_df_captures_malformed_json(self, raw_df):
        """The malformed.json fixture should land in corrupt_df."""
        _, corrupt_df = split_corrupt_records(raw_df)
        assert corrupt_df.count() >= 1, "Expected at least 1 corrupt record"

    def test_valid_df_has_correct_count(self, raw_df):
        """3 valid + 1 missing-field (still valid JSON) = 4 valid rows."""
        valid_df, _ = split_corrupt_records(raw_df)
        assert valid_df.count() == 4, (
            f"Expected 4 valid rows, got {valid_df.count()}"
        )

    def test_missing_field_event_is_in_valid_df(self, raw_df):
        """Null fields in valid JSON should NOT be treated as corrupt."""
        valid_df, _ = split_corrupt_records(raw_df)
        from pyspark.sql.functions import col
        null_uid_rows = valid_df.filter(col("user_id").isNull()).count()
        assert null_uid_rows >= 1, "Missing-field fixture should be in valid_df"


# ══════════════════════════════════════════════════════════════════════
#  Audit column tests
# ══════════════════════════════════════════════════════════════════════


class TestAuditColumns:
    """Verify ingestion_timestamp and source_file are added and non-null."""

    def test_ingestion_timestamp_non_null(self, raw_df):
        from pyspark.sql.functions import col
        valid_df, _ = split_corrupt_records(raw_df)
        null_count = valid_df.filter(col("ingestion_timestamp").isNull()).count()
        assert null_count == 0, "ingestion_timestamp should never be null"

    def test_source_file_non_null(self, raw_df):
        from pyspark.sql.functions import col
        valid_df, _ = split_corrupt_records(raw_df)
        null_count = valid_df.filter(col("source_file").isNull()).count()
        assert null_count == 0, "source_file should never be null"

    def test_source_file_contains_json_extension(self, raw_df):
        """Every source_file value should reference a .json file."""
        from pyspark.sql.functions import col
        valid_df, _ = split_corrupt_records(raw_df)
        non_json = valid_df.filter(~col("source_file").contains(".json")).count()
        assert non_json == 0, "source_file should reference .json files"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
