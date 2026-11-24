"""Read-only client for the Polymarket Gamma (market discovery) API."""

from __future__ import annotations

import json
import logging
from typing import Iterator

import httpx

from ..util.http import get_json

log = logging.getLogger(__name__)

BASE = "https://gamma-api.polymarket.com"
WEATHER_TAG = "daily-temperature"


def iter_events(client: httpx.Client, *, page_size: int = 100, max_pages: int = 1000, **params) -> Iterator[dict]:
    """Yield events from GET /events/keyset, following the cursor to the end.

    Pages above 50 items are served gzip-only; the shared client always asks for gzip.
    """
    cursor: str | None = None
    for _ in range(max_pages):
        q = {"limit": page_size, **params}
        if cursor:
            q["after_cursor"] = cursor
        page = get_json(client, f"{BASE}/events/keyset", params=q)
        events = page.get("events") or []
        yield from events
        cursor = page.get("next_cursor")
        if not cursor or not events:
            return
    log.warning("iter_events stopped at max_pages=%d", max_pages)


def weather_events(client: httpx.Client, *, closed: bool, end_date_min: str | None = None) -> Iterator[dict]:
    params: dict = {"tag_slug": WEATHER_TAG, "closed": "true" if closed else "false"}
    if end_date_min:
        params["end_date_min"] = end_date_min
    return iter_events(client, **params)


def event_by_slug(client: httpx.Client, slug: str) -> dict | None:
    r = get_json(client, f"{BASE}/events", params={"slug": slug})
    return r[0] if r else None


def token_ids(market: dict) -> tuple[str, str] | None:
    """(YES token, NO token) of a binary market, or None if not available."""
    raw = market.get("clobTokenIds")
    try:
        ids = json.loads(raw) if isinstance(raw, str) else raw
        outcomes = market.get("outcomes")
        outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
    except (TypeError, ValueError):
        return None
    if not ids or len(ids) != 2 or not outcomes or len(outcomes) != 2:
        return None
    names = [str(o).strip().lower() for o in outcomes]
    if names == ["yes", "no"]:
        return str(ids[0]), str(ids[1])
    if names == ["no", "yes"]:
        return str(ids[1]), str(ids[0])
    return None


def winner(market: dict) -> str | None:
    """'YES' / 'NO' for a resolved binary market, else None."""
    if not market.get("closed"):
        return None
    raw = market.get("outcomePrices")
    try:
        prices = [float(x) for x in (json.loads(raw) if isinstance(raw, str) else raw)]
        outcomes = market.get("outcomes")
        outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
        names = [str(o).strip().lower() for o in outcomes]
    except (TypeError, ValueError):
        return None
    if len(prices) != 2 or sorted(prices) != [0.0, 1.0]:
        return None
    return names[prices.index(1.0)].upper()
