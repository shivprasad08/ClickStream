import duckdb
from fastapi import FastAPI, HTTPException
import os

app = FastAPI(title="Clickstream Lakehouse API")

STORAGE_PATH = os.environ.get("STORAGE_PATH", "./storage")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")


def get_duckdb_connection():
    """Build and return a DuckDB connection configured with Delta and S3."""
    con = duckdb.connect()
    con.install_extension("delta")
    con.load_extension("delta")
    
    if MINIO_ENDPOINT:
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        # Configure DuckDB's Secret Manager for MinIO S3 compatibility
        con.sql(f"""
            CREATE SECRET (
                TYPE S3,
                ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
                KEY_ID '{os.environ.get("MINIO_ROOT_USER", "minioadmin")}',
                SECRET '{os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")}',
                USE_SSL false,
                URL_STYLE 'path'
            )
        """)
    return con


@app.get("/health")
def health_check():
    """Simple health check confirming API and DuckDB are up."""
    try:
        con = get_duckdb_connection()
        # Ensure we can run a simple query
        con.sql("SELECT 1")
        return {"status": "ok", "message": "API and DuckDB are healthy"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/event-counts")
def get_event_counts(
    event_type: str = None, 
    start_time: str = None, 
    end_time: str = None, 
    limit: int = 100
):
    """Fetch rolling event counts."""
    try:
        con = get_duckdb_connection()
        query = f"SELECT * FROM delta_scan('{STORAGE_PATH}/gold/event_counts')"
        
        conditions = []
        if event_type:
            # SECURITY WARNING: This uses naive string interpolation for filters.
            # In a production environment, this is a SQL INJECTION RISK! 
            # You should use parameterized queries (e.g. `event_type = ?`) or 
            # allowlist validation against known event_types. Kept as-is for 
            # the demo scope per requirements.
            conditions.append(f"event_type = '{event_type}'")
        if start_time:
            conditions.append(f"window_start >= '{start_time}'")
        if end_time:
            conditions.append(f"window_end <= '{end_time}'")
            
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        query += f" ORDER BY window_start DESC LIMIT {limit}"
        
        result = con.sql(query).df()
        return result.to_dict(orient="records")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/session/{session_id}")
def get_session_metrics(session_id: str):
    """Fetch a single session's metrics by session_id."""
    try:
        con = get_duckdb_connection()
        # SECURITY WARNING: naive string interpolation SQL injection risk here too!
        query = f"""
            SELECT * FROM delta_scan('{STORAGE_PATH}/gold/session_metrics')
            WHERE session_id = '{session_id}'
        """
        result = con.sql(query).df()
        
        if result.empty:
            raise HTTPException(status_code=404, detail="Session not found")
            
        return result.to_dict(orient="records")[0]
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/sessions/top-revenue")
def get_top_revenue_sessions(limit: int = 10):
    """Returns top N sessions by session_revenue descending."""
    try:
        con = get_duckdb_connection()
        query = f"""
            SELECT * FROM delta_scan('{STORAGE_PATH}/gold/session_metrics')
            ORDER BY session_revenue DESC
            LIMIT {limit}
        """
        result = con.sql(query).df()
        return result.to_dict(orient="records")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/quarantine-summary")
def get_quarantine_summary():
    """Queries bronze/quarantine and silver/rejected for data quality summary."""
    try:
        con = get_duckdb_connection()
        
        # Bronze quarantine count
        bronze_query = f"SELECT count(*) as cnt FROM delta_scan('{STORAGE_PATH}/bronze/quarantine')"
        try:
            bronze_count = int(con.sql(bronze_query).fetchone()[0])
        except Exception:
            # Table might not exist yet
            bronze_count = 0
            
        # Silver rejected count
        silver_query = f"SELECT count(*) as cnt FROM delta_scan('{STORAGE_PATH}/silver/rejected')"
        try:
            silver_count = int(con.sql(silver_query).fetchone()[0])
        except Exception:
            # Table might not exist yet
            silver_count = 0
            
        return {
            "structurally_malformed_records (bronze)": bronze_count,
            "validation_rejected_records (silver)": silver_count,
            "total_rejected": bronze_count + silver_count
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
