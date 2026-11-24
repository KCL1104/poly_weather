"""Iowa Environmental Mesonet ASOS archive: METAR history beyond AWC's ~30 days.

IEM text carries no METAR/SPECI label, so routine reports and specials are requested
separately and labelled from the request. IEM is a secondary source: live settlement
checks use AWC text, and IEM fills the past for calibration and retrospective checks.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import logging
import sqlite3
import time

import httpx

from ..stations import Station
from ..util.http import request_with_retry
from ..util.timeutil import UTC
from .metar_store import MetarRow, store

log = logging.getLogger(__name__)

URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
_TYPES = {3: "METAR", 4: "SPECI"}  # IEM report_type codes


def fetch(client: httpx.Client, station: Station, start: dt.date, end: dt.date, report_type: int) -> list[MetarRow]:
    """Reports with start <= UTC date < end for one station and one report type."""
    params = [
        ("station", station.iem_id), ("data", "metar"), ("tz", "Etc/UTC"), ("format", "onlycomma"),
        ("latlon", "no"), ("elev", "no"), ("missing", "M"), ("trace", "T"), ("direct", "no"),
        ("report_type", str(report_type)),
        ("year1", start.year), ("month1", start.month), ("day1", start.day),
        ("year2", end.year), ("month2", end.month), ("day2", end.day),
    ]
    r = request_with_retry(client, "GET", URL, params=params, retries=5, backoff=3.0)
    text = r.text
    if not text.startswith("station,"):
        raise ValueError(f"unexpected IEM response: {text[:120]!r}")
    out = []
    for row in csv.DictReader(io.StringIO(text)):
        raw = (row.get("metar") or "").strip()
        if not raw or raw == "M":
            continue
        t = dt.datetime.strptime(row["valid"], "%Y-%m-%d %H:%M").replace(tzinfo=UTC)
        out.append(MetarRow(station.icao, t, raw, _TYPES[report_type]))
    return out


def backfill(
    conn: sqlite3.Connection,
    client: httpx.Client,
    stations: list[Station],
    start: dt.date,
    end: dt.date,
    *,
    chunk_days: int = 120,
    pause_s: float = 1.0,
) -> int:
    total = 0
    for st in stations:
        cur = start
        while cur < end:
            nxt = min(end, cur + dt.timedelta(days=chunk_days))
            for rt in _TYPES:
                rows = fetch(client, st, cur, nxt, rt)
                total += store(conn, rows, "iem")
                time.sleep(pause_s)
            cur = nxt
        log.info("iem backfill %s done (%d new so far)", st.icao, total)
    return total
