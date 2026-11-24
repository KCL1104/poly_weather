"""Store and read archived METAR reports."""

from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from typing import Iterable

from ..resolution.metar import parse_metar
from ..resolution.settle import Obs
from ..util.timeutil import iso, now_ms, parse_iso


@dataclass(frozen=True)
class MetarRow:
    station: str
    obs_time: dt.datetime  # UTC
    raw_text: str
    metar_type: str | None = None
    receipt_time: str | None = None


def store(conn: sqlite3.Connection, rows: Iterable[MetarRow], source: str, seen_ms: int | None = None) -> int:
    """Insert reports not seen before. Returns how many were new.

    A corrected report has different text and is therefore a new row; the original
    stays, and ``first_seen_ms`` orders the versions.
    """
    seen_ms = seen_ms or now_ms()
    new = 0
    conn.execute("BEGIN")
    try:
        for r in rows:
            raw = " ".join(r.raw_text.split())
            if not raw:
                continue
            p = parse_metar(raw)
            cur = conn.execute(
                """INSERT OR IGNORE INTO metar_obs(station, obs_time, raw_text, metar_type, temp_tenths_c,
                       temp_source, source, first_seen_ms, receipt_time)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (r.station, iso(r.obs_time), raw, r.metar_type or p.label, p.temp_tenths_c, p.temp_source,
                 source, seen_ms, r.receipt_time),
            )
            if cur.rowcount:
                new += 1
            elif r.receipt_time:
                conn.execute(
                    "UPDATE metar_obs SET receipt_time=? WHERE station=? AND obs_time=? AND raw_text=? "
                    "AND receipt_time IS NULL",
                    (r.receipt_time, r.station, iso(r.obs_time), raw),
                )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return new


def load_obs(
    conn: sqlite3.Connection,
    station: str,
    start: dt.datetime,
    end: dt.datetime,
    *,
    sources: tuple[str, ...] | None = None,
) -> list[Obs]:
    """Reports for a station with start <= obs_time < end, as settlement inputs.

    When one observation time has several versions, AWC-sourced text outranks IEM text
    and a later sighting outranks an earlier one, mirroring "the report the resolution
    source shows".
    """
    q = "SELECT obs_time, raw_text, metar_type, source, first_seen_ms FROM metar_obs WHERE station=? AND obs_time>=? AND obs_time<?"
    args: list = [station, iso(start), iso(end)]
    if sources:
        q += f" AND source IN ({','.join('?' * len(sources))})"
        args += list(sources)
    rows = conn.execute(q + " ORDER BY obs_time, first_seen_ms", args).fetchall()
    out = []
    for r in rows:
        rank = (1 if r["source"].startswith("awc") else 0) * 10**15 + r["first_seen_ms"]
        out.append(Obs(parse_iso(r["obs_time"]), r["raw_text"], r["metar_type"], rank))
    return out
