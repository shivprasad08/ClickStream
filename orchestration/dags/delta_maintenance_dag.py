"""
delta_maintenance_dag.py
Periodic maintenance for Delta Lake tables: OPTIMIZE (compaction) and 
VACUUM (old file cleanup). Runs independently of the streaming pipeline 
— streaming jobs keep running continuously in their own containers, 
this DAG just performs housekeeping on the same Delta tables.
"""
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "clickstream-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def _get_maintenance_spark_session(app_name: str):
    """Internal helper to build a SparkSession with Delta and MinIO AWS config."""
    from pyspark.sql import SparkSession
    
    # We load credentials from the environment, matching the main pipeline configuration.
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    minio_pw = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")
    
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A MinIO credentials
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_pw)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def optimize_table(table_path: str):
    """
    Runs Delta's OPTIMIZE command to compact small files produced by 
    frequent streaming micro-batch writes into fewer, larger files — 
    improves read performance for the API layer.
    """
    spark = _get_maintenance_spark_session("delta-maintenance-optimize")
    print(f"Running OPTIMIZE on {table_path} ...")
    spark.sql(f"OPTIMIZE delta.`{table_path}`").show()
    spark.stop()


def vacuum_table(table_path: str, retention_hours: int = 168):
    """
    Removes old file versions beyond the retention window (default 7 
    days) to reclaim storage. Note: retention must be >= the checkpoint 
    interval of any streaming reader still consuming this table, or 
    active streams could break — 168 hours is a safe conservative default.
    """
    spark = _get_maintenance_spark_session("delta-maintenance-vacuum")
    print(f"Running VACUUM on {table_path} with retention {retention_hours} HOURS ...")
    
    # Delta enforces a minimum retention of 168 hours by default.
    # We must explicitly disable the check if retention_hours < 168 (useful for testing)
    if retention_hours < 168:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        
    spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS").show()
    spark.stop()


with DAG(
    dag_id="delta_lake_maintenance",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["clickstream", "maintenance"]
) as dag:

    tables = ["bronze/events", "silver/events", "gold/event_counts", "gold/session_metrics"]
    
    for table in tables:
        # We use a jinja templated variable `{{ var.value.storage_path }}` to fetch 
        # the base path from Airflow variables at runtime.
        target_path = f"{{{{ var.value.get('storage_path', 's3a://clickstream-lakehouse') }}}}/{table}"
        
        optimize_task = PythonOperator(
            task_id=f"optimize_{table.replace('/', '_')}",
            python_callable=optimize_table,
            op_kwargs={"table_path": target_path}
        )
        vacuum_task = PythonOperator(
            task_id=f"vacuum_{table.replace('/', '_')}",
            python_callable=vacuum_table,
            op_kwargs={"table_path": target_path}
        )
        optimize_task >> vacuum_task
