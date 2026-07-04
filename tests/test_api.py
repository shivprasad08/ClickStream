import os
import pytest
from datetime import datetime, timedelta
import tempfile
import shutil

# Set environment variables for tests before importing FastAPI app
test_dir = tempfile.mkdtemp()
os.environ["STORAGE_PATH"] = test_dir
# Clear MINIO_ENDPOINT so we test against local files, not S3
if "MINIO_ENDPOINT" in os.environ:
    del os.environ["MINIO_ENDPOINT"]

from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

@pytest.fixture(scope="module", autouse=True)
def setup_mock_delta_tables():
    """Create local Delta tables for tests using delta-rs."""
    import pandas as pd
    from deltalake.writer import write_deltalake
    
    # 1. Mock gold/event_counts
    now = datetime.utcnow()
    counts_data = pd.DataFrame({
        "event_type": ["page_view", "add_to_cart", "purchase", "page_view"],
        "event_count": [100, 25, 10, 120],
        "window_start": [
            now - timedelta(minutes=5),
            now - timedelta(minutes=5),
            now - timedelta(minutes=5),
            now - timedelta(minutes=10)
        ],
        "window_end": [
            now,
            now,
            now,
            now - timedelta(minutes=5)
        ]
    })
    
    counts_path = os.path.join(test_dir, "gold", "event_counts")
    write_deltalake(counts_path, counts_data)
    
    # 2. Mock gold/session_metrics
    sessions_data = pd.DataFrame({
        "session_id": ["sess-1", "sess-2", "sess-3"],
        "user_id": [101, 102, 103],
        "session_start": [
            now - timedelta(minutes=20),
            now - timedelta(minutes=15),
            now - timedelta(minutes=10)
        ],
        "session_end": [
            now - timedelta(minutes=10),
            now - timedelta(minutes=5),
            now
        ],
        "event_count_in_session": [5, 2, 10],
        "session_revenue": [250.0, 0.0, 800.0]
    })
    
    sessions_path = os.path.join(test_dir, "gold", "session_metrics")
    write_deltalake(sessions_path, sessions_data)
    
    yield  # Tests run here
    
    # Teardown
    shutil.rmtree(test_dir)


def test_health_check():
    """Test /health returns 200"""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_get_event_counts_basic():
    """Test /metrics/event-counts returns expected rows"""
    response = client.get("/metrics/event-counts")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 4
    # Check that sorting is descending by window_start
    assert data[0]["window_start"] >= data[-1]["window_start"]


def test_get_event_counts_filter():
    """Test /metrics/event-counts with event_type filter"""
    response = client.get("/metrics/event-counts?event_type=page_view")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    for row in data:
        assert row["event_type"] == "page_view"


def test_get_session_metrics_not_found():
    """Test /metrics/session/{session_id} returns 404 for a nonexistent ID"""
    response = client.get("/metrics/session/invalid-id-999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Session not found"


def test_get_session_metrics_found():
    """Test /metrics/session/{session_id} returns correct session"""
    response = client.get("/metrics/session/sess-1")
    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == "sess-1"
    assert data["user_id"] == 101
    assert data["session_revenue"] == 250.0


def test_get_top_revenue_sessions():
    """Test /metrics/sessions/top-revenue returns results sorted descending"""
    response = client.get("/metrics/sessions/top-revenue?limit=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    # sess-3 has 800.0, sess-1 has 250.0
    assert data[0]["session_id"] == "sess-3"
    assert data[1]["session_id"] == "sess-1"
    assert data[0]["session_revenue"] == 800.0
    assert data[1]["session_revenue"] == 250.0
