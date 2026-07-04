"""
generator.py
Simulates an e-commerce clickstream and writes JSON events to
data/raw_events/ for downstream Spark Structured Streaming ingestion.

Session management, realistic event sequencing, and configurable bad-data
injection are all handled here so that later pipeline modules have a
faithful -- and intentionally noisy -- input stream to validate against.
"""

import json
import os
import random
import signal
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from schemas import generate_event, EVENT_TYPES, NUM_USERS

# ── Configuration ────────────────────────────────────────────────────
load_dotenv()

RAW_EVENTS_PATH: str = os.getenv("RAW_EVENTS_PATH", "./data/raw_events")
EVENT_RATE_PER_SEC: int = int(os.getenv("EVENT_RATE_PER_SEC", "10"))
SESSION_TIMEOUT_MINUTES: int = int(os.getenv("SESSION_TIMEOUT_MINUTES", "30"))

# ── Event-type distribution weights (before purchase constraint) ─────
EVENT_WEIGHTS: dict[str, float] = {
    "page_view": 0.50,
    "search": 0.15,
    "add_to_cart": 0.15,
    "remove_from_cart": 0.05,
    "purchase": 0.15,
}

# Pre-compute for random.choices()
_EVENT_TYPE_LIST = list(EVENT_WEIGHTS.keys())
_EVENT_WEIGHT_LIST = list(EVENT_WEIGHTS.values())

# ── Global state ─────────────────────────────────────────────────────
# {user_id: {"session_id": str, "last_event_time": datetime,
#             "has_added_to_cart": bool}}
active_sessions: dict[int, dict] = {}

# Injection counters
injection_counts: dict[str, int] = {
    "missing_field": 0,
    "bad_timestamp": 0,
    "duplicate": 0,
}

# Tracks the most-recently emitted event for duplicate injection
last_event: dict | None = None

# Total events written (not counting skipped ones)
total_events: int = 0

# Graceful-shutdown flag
_shutdown_requested: bool = False


# ── Signal handling ──────────────────────────────────────────────────
def _handle_signal(signum: int, _frame) -> None:
    """Set the shutdown flag so the main loop exits cleanly."""
    global _shutdown_requested
    _shutdown_requested = True
    sig_name = signal.Signals(signum).name
    print(f"\n[SIGNAL] Received {sig_name}, finishing current event...")


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ══════════════════════════════════════════════════════════════════════
#  Session helpers
# ══════════════════════════════════════════════════════════════════════

def _pick_user_id() -> int:
    """Pick a user_id with 80 % chance of reusing an active user."""
    if active_sessions and random.random() < 0.80:
        return random.choice(list(active_sessions.keys()))
    return random.randint(1, NUM_USERS)


def _resolve_session(user_id: int) -> str:
    """Return (and possibly create/refresh) a session_id for *user_id*."""
    now = datetime.now(timezone.utc)
    timeout = timedelta(minutes=SESSION_TIMEOUT_MINUTES)

    if user_id in active_sessions:
        info = active_sessions[user_id]
        if now - info["last_event_time"] <= timeout:
            # Session still alive -- just touch the timestamp
            info["last_event_time"] = now
            return info["session_id"]
        # Session expired
        print(
            f"[SESSION] Expired session {info['session_id'][:8]}.. "
            f"for user {user_id}"
        )

    # Create a brand-new session
    new_sid = str(uuid.uuid4())
    active_sessions[user_id] = {
        "session_id": new_sid,
        "last_event_time": now,
        "has_added_to_cart": False,
    }
    print(f"[SESSION] Created session {new_sid[:8]}.. for user {user_id}")
    return new_sid


def _pick_event_type(user_id: int) -> str:
    """Choose an event type respecting realism constraints.

    * New sessions start with ``page_view`` 70 % of the time.
    * ``purchase`` is only eligible if the user has already added to cart
      in the current session; otherwise we re-sample.
    """
    info = active_sessions.get(user_id)

    # Brand-new session: bias toward page_view
    if info and info["last_event_time"] == datetime.now(timezone.utc):
        # This is a heuristic: if the session was *just* created this
        # invocation (within the same second), treat it as first event.
        pass  # fall through; handled below via event count proxy

    # Check if this is the very first event in the session (no prior
    # event has been emitted yet -- we proxy this by checking if last_event
    # time equals creation time, but a simpler approach: store a counter).
    is_first_event = info is not None and not info.get("_event_count", 0)

    if is_first_event and random.random() < 0.70:
        return "page_view"

    # Weighted draw with purchase guard
    for _ in range(10):  # up to 10 resamples to avoid purchase on empty cart
        chosen = random.choices(_EVENT_TYPE_LIST, _EVENT_WEIGHT_LIST, k=1)[0]
        if chosen == "purchase" and (
            info is None or not info.get("has_added_to_cart", False)
        ):
            continue  # not eligible -- resample
        return chosen

    # Fallback after 10 failed resamples (extremely unlikely)
    return "page_view"


# ══════════════════════════════════════════════════════════════════════
#  Bad-data injection
# ══════════════════════════════════════════════════════════════════════

def inject_missing_field(event: dict, probability: float = 0.02) -> dict:
    """Randomly null out ``user_id`` or ``timestamp``."""
    if random.random() < probability:
        field = random.choice(["user_id", "timestamp"])
        event[field] = None
        injection_counts["missing_field"] += 1
        print(f'[INJECTED] missing_field ({field}) on event {event["event_id"][:8]}..')
    return event


def inject_bad_timestamp(event: dict, probability: float = 0.03) -> dict:
    """Replace ``timestamp`` with one ±2 hours from now."""
    if random.random() < probability:
        direction = random.choice([-1, 1])
        skewed = datetime.now(timezone.utc) + timedelta(hours=2 * direction)
        event["timestamp"] = (
            skewed.strftime("%Y-%m-%dT%H:%M:%S.")
            + f"{skewed.microsecond // 1000:03d}Z"
        )
        label = "future" if direction == 1 else "stale"
        injection_counts["bad_timestamp"] += 1
        print(
            f'[INJECTED] bad_timestamp ({label}) on event '
            f'{event["event_id"][:8]}..'
        )
    return event


def inject_duplicate(event: dict, probability: float = 0.01) -> dict:
    """With *probability*, re-emit the previous event verbatim."""
    global last_event
    if last_event is not None and random.random() < probability:
        injection_counts["duplicate"] += 1
        eid = last_event["event_id"][:8]
        print(f"[INJECTED] duplicate on event {eid}..")
        return last_event.copy()
    return event


# ══════════════════════════════════════════════════════════════════════
#  File writing (atomic)
# ══════════════════════════════════════════════════════════════════════

def _write_event(event: dict) -> None:
    """Write *event* as a single-line JSON file using atomic rename.

    Filename: ``{unix_timestamp_ms}_{event_id}.json``
    """
    ts_ms = int(time.time() * 1000)
    filename = f"{ts_ms}_{event['event_id']}.json"
    final_path = os.path.join(RAW_EVENTS_PATH, filename)
    tmp_path = final_path + ".tmp"

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(event, f)

    os.replace(tmp_path, final_path)


# ══════════════════════════════════════════════════════════════════════
#  Main loop
# ══════════════════════════════════════════════════════════════════════

def _log_summary() -> None:
    """Print a periodic status line to stdout."""
    print(
        f"[SUMMARY] total_events={total_events}  "
        f"injections={injection_counts}  "
        f"active_sessions={len(active_sessions)}"
    )


def main() -> None:
    global total_events, last_event

    # Ensure output directory exists
    Path(RAW_EVENTS_PATH).mkdir(parents=True, exist_ok=True)

    sleep_interval = 1.0 / EVENT_RATE_PER_SEC
    print(
        f"[STARTUP] Generating events at {EVENT_RATE_PER_SEC}/s -> "
        f"{RAW_EVENTS_PATH}"
    )

    while not _shutdown_requested:
        # ── 1. Pick user & session ───────────────────────────────────
        user_id = _pick_user_id()
        session_id = _resolve_session(user_id)

        # ── 2. Choose event type ─────────────────────────────────────
        event_type = _pick_event_type(user_id)

        # ── 3. Build event dict ──────────────────────────────────────
        event = generate_event(
            user_id=user_id,
            session_id=session_id,
            event_type=event_type,
        )

        # ── 4. Update session bookkeeping ────────────────────────────
        info = active_sessions.get(user_id)
        if info:
            info["_event_count"] = info.get("_event_count", 0) + 1
            if event_type == "add_to_cart":
                info["has_added_to_cart"] = True

        # ── 5. Apply bad-data injections (in sequence) ───────────────
        event = inject_missing_field(event)
        event = inject_bad_timestamp(event)
        event = inject_duplicate(event)

        # ── 6. Write to disk ─────────────────────────────────────────
        _write_event(event)
        last_event = event.copy()
        total_events += 1

        # ── 7. Periodic summary ──────────────────────────────────────
        if total_events % 100 == 0:
            _log_summary()

        # ── 8. Throttle ─────────────────────────────────────────────
        time.sleep(sleep_interval)

    # ── Shutdown ─────────────────────────────────────────────────────
    print(f"[SHUTDOWN] Shutting down, final count: {total_events} events")
    _log_summary()
    sys.exit(0)


if __name__ == "__main__":
    main()
