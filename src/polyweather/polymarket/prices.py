"""Price-history archive from the public Data API.

The exchange keeps one-minute history for about 7 days, five-minute for about 60 days
and thirty-minute for about 90 days. This is a mid/last-price series with no depth,
so it cannot stand in for the order-book recorder; it is a backstop for recorder
gaps and the input of the retrospective study.
"""

from __future__ import annotations

import datetime as dt
import logging
import sqlite3
import time
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from ..util.http import request_with_retry
from ..util.timeutil import UTC, now_ms, parse_iso

log = logging.getLogger(__name__)

URL = "https://data-api.polymarket.com/v2/prices-history"
MAX_SPAN_S = 15 * 86400

_SCHEMA = pa.schema(
    [
        ("asset_id", pa.string()),
        ("market_id", pa.string()),
        ("ts", pa.int64()),  # epoch seconds
        ("price", pa.float64()),
        ("resolution_s", pa.int32()),
    ]
)


def fetch_history(
    client: httpx.Client, token_id: str, start: int, end: int, bucket_seconds: int | None = None
) -> list[tuple[int, float, int]]:
    """(ts, price, resolution_seconds) points with start <= ts < end; span at most 15 days."""
    out: list[tuple[int, float, int]] = []
    params: dict = {"token_id": token_id, "start": start, "end": end}
    if bucket_seconds:
        params["bucket_seconds"] = bucket_seconds
    cursor = None
    for _ in range(200):
        q = dict(params)
        if cursor:
            q["cursor"] = cursor
        page = request_with_retry(client, "GET", URL, params=q).json()
        for p in page.get("data") or []:
            out.append((int(p["timestamp"]), float(p["price"]), int(p.get("resolution_seconds") or 0)))
        pg = page.get("pagination") or {}
        cursor = pg.get("next_cursor")
        if not pg.get("has_more") or not cursor:
            break
    return out


def archive_events(
    conn: sqlite3.Connection,
    client: httpx.Client,
    data_dir: Path,
    event_ids: list[str],
    *,
    bucket_seconds: int = 60,
    label: str = "1m",
    pause_s: float = 0.05,
) -> tuple[int, Path | None]:
    """Fetch YES-token history for every market of the given events into one Parquet file."""
    cols: dict[str, list] = {f.name: [] for f in _SCHEMA}
    n_tokens = 0
    for event_id in event_ids:
        ev = conn.execute("SELECT start_date, end_date, closed_time FROM events WHERE event_id=?", (event_id,)).fetchone()
        if ev is None or not ev["start_date"]:
            continue
        start = int(parse_iso(ev["start_date"]).timestamp())
        stop_iso = ev["closed_time"] or ev["end_date"]
        stop = int(parse_iso(stop_iso).timestamp()) + 6 * 3600 if stop_iso else int(time.time())
        stop = min(stop, int(time.time()))
        for m in conn.execute("SELECT market_id, yes_token FROM markets WHERE event_id=? AND yes_token IS NOT NULL", (event_id,)):
            t0 = start
            while t0 < stop:
                t1 = min(stop, t0 + MAX_SPAN_S)
                try:
                    points = fetch_history(client, m["yes_token"], t0, t1, bucket_seconds)
                except httpx.HTTPError as e:
                    log.warning("price history failed for market %s: %s", m["market_id"], e)
                    points = []
                for ts, price, res in points:
                    cols["asset_id"].append(m["yes_token"])
                    cols["market_id"].append(m["market_id"])
                    cols["ts"].append(ts)
                    cols["price"].append(price)
                    cols["resolution_s"].append(res)
                t0 = t1
                time.sleep(pause_s)
            n_tokens += 1
    if not cols["ts"]:
        return n_tokens, None
    day = dt.datetime.now(UTC).strftime("%Y-%m-%d")
    path = data_dir / "raw" / "prices_history" / f"date={day}" / f"prices-{label}-{now_ms()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    pq.write_table(pa.Table.from_pydict(cols, schema=_SCHEMA), tmp, compression="zstd")
    tmp.replace(path)
    return n_tokens, path
