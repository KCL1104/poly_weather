"""aviationweather.gov METAR archive: the resolution source itself.

Two feeds are used. The cache file lists the latest report of every station in the
world and is refreshed about once a minute, so one small download per minute covers
all tracked stations. The Data API returns history (roughly the last 30 days, at most
400 reports per request) together with the time AWC received each report.
"""

from __future__ import annotations

import csv
import datetime as dt
import gzip
import io
import logging
import sqlite3
import time
from typing import Iterable, Sequence

import httpx

from ..util.http import request_with_retry
from ..util.timeutil import UTC, iso, now_ms, parse_iso, utcnow
from .metar_store import MetarRow, store

log = logging.getLogger(__name__)

API = "https://aviationweather.gov/api/data/metar"
CACHE = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
API_MAX_ROWS = 400


def fetch_api(
    client: httpx.Client, ids: Sequence[str], *, hours: float, end: dt.datetime | None = None
) -> list[MetarRow]:
    params = {"ids": ",".join(ids), "format": "json", "hours": f"{hours:g}"}
    if end is not None:
        params["date"] = iso(end)
    r = request_with_retry(client, "GET", API, params=params, ok=(200, 204))
    if r.status_code == 204 or not r.content.strip():
        return []
    out = []
    for it in r.json():
        raw = it.get("rawOb")
        if not raw or it.get("obsTime") is None:
            continue
        out.append(
            MetarRow(
                station=it["icaoId"],
                obs_time=dt.datetime.fromtimestamp(int(it["obsTime"]), UTC),
                raw_text=raw,
                metar_type=it.get("metarType"),
                receipt_time=it.get("receiptTime"),
            )
        )
    return out


def parse_cache(data: bytes, wanted: set[str]) -> list[MetarRow]:
    text = gzip.decompress(data).decode("utf-8", "replace")
    reader = csv.reader(io.StringIO(text))
    header = next(reader)
    col = {name: i for i, name in enumerate(header)}  # duplicate names: last wins, unused here
    i_raw, i_st, i_time = header.index("raw_text"), header.index("station_id"), header.index("observation_time")
    i_type = col.get("metar_type")
    out = []
    for row in reader:
        if len(row) <= i_time or row[i_st] not in wanted:
            continue
        try:
            t = parse_iso(row[i_time])
        except ValueError:
            continue
        mtype = row[i_type] if i_type is not None and len(row) > i_type else None
        out.append(MetarRow(row[i_st], t, row[i_raw], mtype or None))
    return out


def fetch_cache(client: httpx.Client, wanted: set[str]) -> list[MetarRow]:
    r = request_with_retry(client, "GET", CACHE)
    return parse_cache(r.content, wanted)


def backfill(
    conn: sqlite3.Connection,
    client: httpx.Client,
    ids: Iterable[str],
    *,
    days: float = 30.0,
    window_hours: float = 72.0,
    pause_s: float = 0.7,
) -> int:
    """Pull whatever history the API still serves for each station."""
    total = 0
    end0 = utcnow()
    for station in ids:
        end = end0
        oldest = end0 - dt.timedelta(days=days)
        while end > oldest:
            hours = window_hours
            rows = fetch_api(client, [station], hours=hours, end=end)
            while len(rows) >= API_MAX_ROWS and hours > 6:
                hours /= 2
                rows = fetch_api(client, [station], hours=hours, end=end)
                time.sleep(pause_s)
            total += store(conn, rows, "awc_api")
            end -= dt.timedelta(hours=hours)
            time.sleep(pause_s)  # the API allows 100 requests a minute
        log.info("awc backfill %s done (%d new so far)", station, total)
    return total


def poll_forever(
    conn: sqlite3.Connection,
    client: httpx.Client,
    ids: Sequence[str],
    *,
    cache_every_s: float = 60.0,
    api_every_s: float = 600.0,
    stop_after_s: float | None = None,
) -> None:
    """Archive loop: cache file every minute, API reconciliation every ten minutes.

    The API pass fills in AWC receipt times and anything the cache polling missed
    (the cache only holds each station's latest report).
    """
    wanted = set(ids)
    started = time.monotonic()
    next_api = 0.0
    gap_open: int | None = None
    last_ok_ms = now_ms()
    while True:
        tick = time.monotonic()
        try:
            new = store(conn, fetch_cache(client, wanted), "awc_cache")
            if tick >= next_api:
                rows: list[MetarRow] = []
                for i in range(0, len(ids), 40):
                    rows += fetch_api(client, ids[i : i + 40], hours=3)
                new += store(conn, rows, "awc_api")
                next_api = tick + api_every_s
            if gap_open is not None:
                conn.execute("UPDATE gaps SET end_ms=? WHERE id=?", (now_ms(), gap_open))
                gap_open = None
            last_ok_ms = now_ms()
            if new:
                log.info("metar: %d new reports", new)
        except (httpx.HTTPError, ValueError, OSError) as e:
            log.warning("metar poll failed: %s", e)
            if gap_open is None and now_ms() - last_ok_ms > 5 * 60_000:
                cur = conn.execute(
                    "INSERT INTO gaps(source, key, start_ms, reason) VALUES('metar', 'awc', ?, ?)",
                    (last_ok_ms, f"poll failing: {e}"[:300]),
                )
                gap_open = cur.lastrowid
        if stop_after_s is not None and time.monotonic() - started >= stop_after_s:
            return
        time.sleep(max(1.0, cache_every_s - (time.monotonic() - tick)))
