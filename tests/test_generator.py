"""
test_generator.py
Unit tests for the clickstream event generator (producer module).
"""

import os
import sys
import uuid
import random
import time
import shutil
import json
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

# ── Make producer/ importable ────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))

from schemas import generate_event, ClickstreamEvent, EVENT_TYPES, NUM_USERS, NUM_PRODUCTS
from generator import (
    inject_missing_field,
    inject_bad_timestamp,
    inject_duplicate,
    active_sessions,
    _resolve_session,
    _pick_user_id,
    _write_event,
)


# ══════════════════════════════════════════════════════════════════════
#  Schema / generate_event tests
# ══════════════════════════════════════════════════════════════════════


class TestGenerateEvent:
    """Verify generate_event() produces all required fields per schema."""

    def test_required_fields_present(self):
        event = generate_event(user_id=1, session_id="s-1", event_type="page_view")
        required = {"event_id", "user_id", "session_id", "event_type", "timestamp", "metadata"}
        assert required.issubset(event.keys()), f"Missing keys: {required - event.keys()}"

    def test_event_id_is_uuid4(self):
        event = generate_event(user_id=1, session_id="s-1", event_type="page_view")
        # Should parse without error
        parsed = uuid.UUID(event["event_id"], version=4)
        assert str(parsed) == event["event_id"]

    def test_user_id_passed_through(self):
        event = generate_event(user_id=42, session_id="s-1", event_type="page_view")
        assert event["user_id"] == 42

    def test_session_id_passed_through(self):
        sid = str(uuid.uuid4())
        event = generate_event(user_id=1, session_id=sid, event_type="page_view")
        assert event["session_id"] == sid

    def test_event_type_passed_through(self):
        for et in EVENT_TYPES:
            event = generate_event(user_id=1, session_id="s-1", event_type=et)
            assert event["event_type"] == et

    def test_timestamp_is_iso8601_utc(self):
        event = generate_event(user_id=1, session_id="s-1", event_type="page_view")
        ts = event["timestamp"]
        assert ts.endswith("Z"), "Timestamp must end with Z (UTC)"
        # Should parse without error
        datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")

    def test_metadata_has_device_and_referrer(self):
        event = generate_event(user_id=1, session_id="s-1", event_type="page_view")
        meta = event["metadata"]
        assert "device_type" in meta
        assert "referrer" in meta
        assert meta["device_type"] in ["mobile", "desktop", "tablet"]
        assert meta["referrer"] in ["direct", "google", "facebook", "email", "instagram"]

    def test_product_id_present_for_non_search(self):
        for et in ["page_view", "add_to_cart", "remove_from_cart", "purchase"]:
            event = generate_event(user_id=1, session_id="s-1", event_type=et)
            assert "product_id" in event, f"product_id missing for {et}"
            assert 1 <= event["product_id"] <= NUM_PRODUCTS

    def test_product_id_absent_for_search(self):
        event = generate_event(user_id=1, session_id="s-1", event_type="search")
        assert "product_id" not in event, "search events must not have product_id"

    def test_purchase_includes_price(self):
        event = generate_event(user_id=1, session_id="s-1", event_type="purchase")
        assert "price" in event["metadata"], "purchase events must include price"
        price = event["metadata"]["price"]
        assert 5.00 <= price <= 500.00

    def test_non_purchase_excludes_price(self):
        for et in ["page_view", "add_to_cart", "remove_from_cart", "search"]:
            event = generate_event(user_id=1, session_id="s-1", event_type=et)
            assert "price" not in event["metadata"], (
                f"price should be absent for {et}"
            )

    def test_dataclass_to_dict_round_trip(self):
        """Ensure ClickstreamEvent.to_dict() drops None product_id."""
        obj = ClickstreamEvent(
            event_id="x", user_id=1, session_id="s",
            event_type="search", product_id=None,
            timestamp="t", metadata={},
        )
        d = obj.to_dict()
        assert "product_id" not in d


# ══════════════════════════════════════════════════════════════════════
#  Bad-data injection tests
# ══════════════════════════════════════════════════════════════════════


class TestInjections:
    """Verify injection functions fire at approximately the right rates."""

    @staticmethod
    def _make_event():
        return generate_event(user_id=1, session_id="s-1", event_type="page_view")

    def test_inject_missing_field_statistical(self):
        """Over 1000 trials at p=0.02, expect ~20 hits (tolerance 5-50)."""
        hits = 0
        for _ in range(1000):
            event = self._make_event()
            original_uid = event["user_id"]
            original_ts = event["timestamp"]
            event = inject_missing_field(event, probability=0.02)
            if event["user_id"] is None or event["timestamp"] is None:
                hits += 1
        # Binomial(1000, 0.02): mean=20, ~99.7% CI ≈ [5, 50]
        assert 5 <= hits <= 50, f"Expected ~20 hits, got {hits}"

    def test_inject_missing_field_nulls_correct_fields(self):
        """When it fires, only user_id or timestamp should be nulled."""
        random.seed(42)
        nulled_fields = set()
        for _ in range(5000):
            event = self._make_event()
            event = inject_missing_field(event, probability=1.0)  # force fire
            if event["user_id"] is None:
                nulled_fields.add("user_id")
            if event["timestamp"] is None:
                nulled_fields.add("timestamp")
        assert nulled_fields == {"user_id", "timestamp"}

    def test_inject_bad_timestamp_shifts_time(self):
        """When fired, timestamp should differ from now by ~2 hours."""
        event = self._make_event()
        original_ts = event["timestamp"]
        modified = inject_bad_timestamp(event, probability=1.0)
        assert modified["timestamp"] != original_ts

    def test_inject_duplicate_returns_previous_event(self):
        """When fired, should return a copy of the last_event."""
        import generator

        first = self._make_event()
        generator.last_event = first.copy()

        second = self._make_event()
        result = inject_duplicate(second, probability=1.0)

        assert result["event_id"] == first["event_id"]
        assert result is not first  # must be a copy, not the same object


# ══════════════════════════════════════════════════════════════════════
#  Session management tests
# ══════════════════════════════════════════════════════════════════════


class TestSessionManagement:
    """Verify session_id persistence and expiry."""

    def setup_method(self):
        active_sessions.clear()

    def test_session_persists_within_timeout(self):
        """Same user within timeout window should reuse session_id."""
        sid1 = _resolve_session(1)
        sid2 = _resolve_session(1)
        assert sid1 == sid2, "Session should persist for same user"

    def test_new_session_after_timeout(self):
        """Session should expire after SESSION_TIMEOUT_MINUTES."""
        sid1 = _resolve_session(1)
        # Manually age the session beyond timeout
        active_sessions[1]["last_event_time"] = (
            datetime.now(timezone.utc) - timedelta(minutes=31)
        )
        sid2 = _resolve_session(1)
        assert sid1 != sid2, "Session should expire after timeout"

    def test_different_users_get_different_sessions(self):
        sid1 = _resolve_session(1)
        sid2 = _resolve_session(2)
        assert sid1 != sid2

    def test_pick_user_id_reuse_bias(self):
        """With active sessions, _pick_user_id should reuse ~80%."""
        active_sessions.clear()
        # Seed a few sessions
        for uid in [1, 2, 3]:
            _resolve_session(uid)

        random.seed(0)
        reused = sum(
            1 for _ in range(1000)
            if _pick_user_id() in active_sessions
        )
        # Expect ~800 reuses.  Allow [700, 950] for statistical tolerance.
        assert 700 <= reused <= 950, f"Expected ~800 reuses, got {reused}"


# ══════════════════════════════════════════════════════════════════════
#  Atomic file-writing test
# ══════════════════════════════════════════════════════════════════════


class TestFileWriting:
    """Verify events are written as single-line JSON files."""

    TMP_DIR = os.path.join(os.path.dirname(__file__), "_test_raw_events")

    def setup_method(self):
        os.makedirs(self.TMP_DIR, exist_ok=True)

    def teardown_method(self):
        shutil.rmtree(self.TMP_DIR, ignore_errors=True)

    def test_write_event_creates_json_file(self):
        event = generate_event(user_id=1, session_id="s-1", event_type="page_view")
        with mock.patch("generator.RAW_EVENTS_PATH", self.TMP_DIR):
            _write_event(event)

        files = [f for f in os.listdir(self.TMP_DIR) if f.endswith(".json")]
        assert len(files) == 1

        with open(os.path.join(self.TMP_DIR, files[0])) as f:
            loaded = json.load(f)
        assert loaded["event_id"] == event["event_id"]

    def test_no_tmp_files_remain(self):
        """Atomic write should leave no .tmp files behind."""
        event = generate_event(user_id=1, session_id="s-1", event_type="page_view")
        with mock.patch("generator.RAW_EVENTS_PATH", self.TMP_DIR):
            _write_event(event)

        tmp_files = [f for f in os.listdir(self.TMP_DIR) if f.endswith(".tmp")]
        assert len(tmp_files) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
