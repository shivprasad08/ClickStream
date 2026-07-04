"""
spark_session.py
Shared SparkSession factory for all lakehouse layer jobs
(Bronze, Silver, Gold).

Every job calls ``get_spark_session(app_name)`` with a distinct
*app_name* so they are easy to tell apart in the Spark UI and logs,
while sharing identical Delta Lake / shuffle configuration.
"""

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")


def get_spark_session(app_name: str) -> SparkSession:
    """Build a SparkSession configured for Delta Lake support.

    Parameters
    ----------
    app_name : str
        Human-readable name for this job (e.g. ``"bronze-layer"``).
        Appears in the Spark UI and driver logs.

    Returns
    -------
    SparkSession
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        # ── Delta Lake extensions ────────────────────────────────
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── Schema auto-merge (needed for Silver/Gold evolution) ─
        .config(
            "spark.databricks.delta.schema.autoMerge.enabled",
            "true",
        )
        # ── Local-dev tuning: avoid 200-partition default ────────
        .config("spark.sql.shuffle.partitions", "4")
        # ── S3A configuration for MinIO ──────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    return spark
