"""
init_minio.py
Creates the clickstream-lakehouse bucket in MinIO if it doesn't exist.
Run once at stack startup before any Spark job starts.
"""
from minio import Minio
import os

def ensure_bucket_exists():
    client = Minio(
        os.environ["MINIO_ENDPOINT"].replace("http://", ""),
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False
    )
    bucket = os.environ["MINIO_BUCKET"]
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Created bucket: {bucket}")
    else:
        print(f"Bucket already exists: {bucket}")

if __name__ == "__main__":
    ensure_bucket_exists()
