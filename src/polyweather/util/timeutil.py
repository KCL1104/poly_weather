from __future__ import annotations

import datetime as dt
import time

UTC = dt.timezone.utc


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def utcnow() -> dt.datetime:
    return dt.datetime.now(UTC)


def parse_iso(s: str) -> dt.datetime:
    """Parse an ISO-8601 timestamp; a naive value is taken as UTC."""
    s = s.strip().replace("Z", "+00:00")
    if " " in s and "T" not in s:
        s = s.replace(" ", "T", 1)
    d = dt.datetime.fromisoformat(s)
    return d if d.tzinfo else d.replace(tzinfo=UTC)


def iso(d: dt.datetime) -> str:
    """Canonical UTC ISO string with second precision, e.g. 2026-11-09T20:51:00Z."""
    return d.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def ms_to_iso(ms: int) -> str:
    return iso(dt.datetime.fromtimestamp(ms / 1000, UTC))
