"""
schemas.py
Defines the ClickstreamEvent dataclass and a factory function to build
a fully-populated event dict ready for JSON serialisation.
"""

import uuid
import random
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional


# ── Constants ────────────────────────────────────────────────────────
EVENT_TYPES: list[str] = [
    "page_view",
    "add_to_cart",
    "remove_from_cart",
    "search",
    "purchase",
]

DEVICE_TYPES: list[str] = ["mobile", "desktop", "tablet"]
REFERRERS: list[str] = ["direct", "google", "facebook", "email", "instagram"]

NUM_USERS: int = 500        # user_id drawn from 1..500
NUM_PRODUCTS: int = 1000    # product_id drawn from 1..1000

PRICE_MIN: float = 5.00
PRICE_MAX: float = 500.00


# ── Dataclass ────────────────────────────────────────────────────────
@dataclass
class ClickstreamEvent:
    """Schema for a single e-commerce clickstream event."""

    event_id: str
    user_id: int
    session_id: str
    event_type: str
    product_id: Optional[int]
    timestamp: str
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Return a plain dict suitable for ``json.dumps``.

        ``product_id`` is excluded when *None* so downstream consumers
        see a genuinely absent key rather than a null value.
        """
        d = asdict(self)
        if d["product_id"] is None:
            del d["product_id"]
        return d


# ── Factory ──────────────────────────────────────────────────────────
def generate_event(
    user_id: int,
    session_id: str,
    event_type: str,
    product_id: Optional[int] = None,
) -> dict:
    """Build one valid event dict per the clickstream schema.

    Parameters
    ----------
    user_id : int
        Simulated user identifier (1-500).
    session_id : str
        UUID4 string representing the current session.
    event_type : str
        One of the valid ``EVENT_TYPES``.
    product_id : int | None
        Product identifier.  Automatically set to *None* for ``"search"``
        events, and randomly generated (1-1000) for all other types when
        not explicitly provided.

    Returns
    -------
    dict
        A JSON-serialisable event dictionary.
    """
    # Resolve product_id: search events never carry one
    if event_type == "search":
        product_id = None
    elif product_id is None:
        product_id = random.randint(1, NUM_PRODUCTS)

    # ISO 8601 UTC timestamp with millisecond precision
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"

    # Build metadata
    meta: dict = {
        "device_type": random.choice(DEVICE_TYPES),
        "referrer": random.choice(REFERRERS),
    }
    if event_type == "purchase":
        meta["price"] = round(random.uniform(PRICE_MIN, PRICE_MAX), 2)

    event = ClickstreamEvent(
        event_id=str(uuid.uuid4()),
        user_id=user_id,
        session_id=session_id,
        event_type=event_type,
        product_id=product_id,
        timestamp=ts,
        metadata=meta,
    )

    return event.to_dict()
