"""
test_gold_layer.py
Unit tests for the Gold layer aggregations and MERGE logic.

Tests use batch mode against in-memory DataFrames mimicking Silver output.
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

from gold_layer import (
    compute_event_counts,
    compute_session_metrics,
    ensure_gold_table_exists,
    make_upsert_session_metrics,
)

# ── Silver-shaped schema ─────────────────────────────────────────────

SILVER_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("device_type", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_date", DateType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("source_file", StringType(), True),
])


# ── Spark session ────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_gold_layer")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


# ── Test data ────────────────────────────────────────────────────────
# Events across two 5-minute windows and two sessions:
# Window 1: 10:00 - 10:05
# Window 2: 10:05 - 10:10
#
# Session 1 (s1): 1 page_view, 1 add_to_cart, 1 purchase ($100) -> 3 events
# Session 2 (s2): 2 page_views -> 2 events

import datetime

def ts(dt_str):
    return datetime.datetime.fromisoformat(dt_str)

def dt(dt_str):
    return datetime.date.fromisoformat(dt_str)

TEST_ROWS = [
    # Session 1 (s1)
    Row(event_id="e1", user_id=1, session_id="s1", event_type="page_view", product_id=1,
        event_timestamp=ts("2026-07-04T10:01:00"), device_type="mobile", referrer="direct", price=None,
        event_date=dt("2026-07-04"), ingestion_timestamp=ts("2026-07-04T10:01:01"), source_file="x"),
    Row(event_id="e2", user_id=1, session_id="s1", event_type="add_to_cart", product_id=1,
        event_timestamp=ts("2026-07-04T10:04:00"), device_type="mobile", referrer="direct", price=None,
        event_date=dt("2026-07-04"), ingestion_timestamp=ts("2026-07-04T10:04:01"), source_file="x"),
    Row(event_id="e3", user_id=1, session_id="s1", event_type="purchase", product_id=1,
        event_timestamp=ts("2026-07-04T10:06:00"), device_type="mobile", referrer="direct", price=100.0,
        event_date=dt("2026-07-04"), ingestion_timestamp=ts("2026-07-04T10:06:01"), source_file="x"),
    
    # Session 2 (s2)
    Row(event_id="e4", user_id=2, session_id="s2", event_type="page_view", product_id=2,
        event_timestamp=ts("2026-07-04T10:02:00"), device_type="desktop", referrer="google", price=None,
        event_date=dt("2026-07-04"), ingestion_timestamp=ts("2026-07-04T10:02:01"), source_file="y"),
    Row(event_id="e5", user_id=2, session_id="s2", event_type="page_view", product_id=3,
        event_timestamp=ts("2026-07-04T10:08:00"), device_type="desktop", referrer="google", price=None,
        event_date=dt("2026-07-04"), ingestion_timestamp=ts("2026-07-04T10:08:01"), source_file="y"),
]


@pytest.fixture(scope="module")
def silver_df(spark):
    return spark.createDataFrame(TEST_ROWS, schema=SILVER_SCHEMA)


# ======================================================================
#  Event Counts Tests
# ======================================================================


class TestEventCounts:

    def test_windowed_counts(self, silver_df):
        counts_df = compute_event_counts(silver_df)
        rows = counts_df.collect()
        
        # 10:00-10:05 window: 2 page_views (e1, e4), 1 add_to_cart (e2)
        # 10:05-10:10 window: 1 purchase (e3), 1 page_view (e5)
        
        # Verify 10:00-10:05 page_view
        pv_w1 = [r for r in rows if r.event_type == "page_view" and r.window_start.minute == 0]
        assert len(pv_w1) == 1
        assert pv_w1[0].event_count == 2
        
        # Verify 10:05-10:10 purchase
        pur_w2 = [r for r in rows if r.event_type == "purchase" and r.window_start.minute == 5]
        assert len(pur_w2) == 1
        assert pur_w2[0].event_count == 1
        
    def test_flattened_schema(self, silver_df):
        counts_df = compute_event_counts(silver_df)
        cols = set(counts_df.columns)
        assert "window_start" in cols
        assert "window_end" in cols
        assert "event_type" in cols
        assert "event_count" in cols
        assert "window" not in cols  # struct must be flattened


# ======================================================================
#  Session Metrics Tests
# ======================================================================


class TestSessionMetrics:

    def test_session_aggregations(self, silver_df):
        metrics_df = compute_session_metrics(silver_df)
        rows = metrics_df.orderBy("session_id").collect()
        
        assert len(rows) == 2
        
        s1, s2 = rows[0], rows[1]
        assert s1.session_id == "s1"
        assert s2.session_id == "s2"
        
        # Check start/end bounds
        assert s1.session_start == ts("2026-07-04T10:01:00")
        assert s1.session_end == ts("2026-07-04T10:06:00")
        
        # Check counts
        assert s1.event_count_in_session == 3
        assert s2.event_count_in_session == 2
        
        # Check revenue
        assert s1.session_revenue == 100.0
        assert s2.session_revenue == 0.0


# ======================================================================
#  MERGE INTO (CDC) Tests
# ======================================================================


class TestMergeInto:

    def test_upsert_session_metrics(self, spark, tmp_path):
        target_path = str(tmp_path / "gold_session_metrics")
        
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
        )
        schema = StructType([
            StructField("session_id", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("session_start", TimestampType(), True),
            StructField("session_end", TimestampType(), True),
            StructField("event_count_in_session", IntegerType(), False),
            StructField("session_revenue", DoubleType(), True),
        ])
        
        # 1. Bootstrap empty table
        ensure_gold_table_exists(spark, target_path, schema)
        
        upsert_fn = make_upsert_session_metrics(spark, target_path)
        
        # 2. First microbatch: session s1 has 1 event
        batch1_data = [
            Row(session_id="s1", user_id=1, session_start=ts("2026-07-04T10:00:00"), 
                session_end=ts("2026-07-04T10:00:00"), event_count_in_session=1, session_revenue=0.0)
        ]
        batch1_df = spark.createDataFrame(batch1_data, schema=schema)
        
        upsert_fn(batch1_df, 1)  # RUN MERGE
        
        res1 = spark.read.format("delta").load(target_path).collect()
        assert len(res1) == 1
        assert res1[0].event_count_in_session == 1
        
        # 3. Second microbatch: s1 gets updated (2nd event, purchase), s2 appears
        batch2_data = [
            # s1 updated
            Row(session_id="s1", user_id=1, session_start=ts("2026-07-04T10:00:00"), 
                session_end=ts("2026-07-04T10:05:00"), event_count_in_session=2, session_revenue=50.0),
            # s2 inserted
            Row(session_id="s2", user_id=2, session_start=ts("2026-07-04T10:10:00"), 
                session_end=ts("2026-07-04T10:10:00"), event_count_in_session=1, session_revenue=0.0)
        ]
        batch2_df = spark.createDataFrame(batch2_data, schema=schema)
        
        upsert_fn(batch2_df, 2)  # RUN MERGE
        
        res2 = spark.read.format("delta").load(target_path).orderBy("session_id").collect()
        assert len(res2) == 2  # s1 updated (not duplicated), s2 inserted
        
        # Check s1 update
        assert res2[0].session_id == "s1"
        assert res2[0].event_count_in_session == 2
        assert res2[0].session_revenue == 50.0
        
        # Check s2 insert
        assert res2[1].session_id == "s2"
        assert res2[1].event_count_in_session == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
