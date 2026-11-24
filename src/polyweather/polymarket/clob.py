"""Read-only client for the public CLOB market-data endpoints (no authentication)."""

from __future__ import annotations

from typing import Sequence

import httpx

BASE = "https://clob.polymarket.com"
BOOKS_BATCH = 500  # documented maximum per request


async def get_books(client: httpx.AsyncClient, token_ids: Sequence[str]) -> list[dict]:
    """POST /books: current order books for up to 500 outcomes per request."""
    out: list[dict] = []
    for i in range(0, len(token_ids), BOOKS_BATCH):
        r = await client.post(f"{BASE}/books", json=[{"token_id": t} for t in token_ids[i : i + BOOKS_BATCH]])
        r.raise_for_status()
        out += r.json()
    return out


def get_books_sync(client: httpx.Client, token_ids: Sequence[str]) -> list[dict]:
    out: list[dict] = []
    for i in range(0, len(token_ids), BOOKS_BATCH):
        r = client.post(f"{BASE}/books", json=[{"token_id": t} for t in token_ids[i : i + BOOKS_BATCH]])
        r.raise_for_status()
        out += r.json()
    return out
