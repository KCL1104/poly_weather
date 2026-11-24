"""NOAA GFS LAMP station bulletins: hourly temperature guidance out to 38 hours.

A cycle runs every hour at HH:30 UTC and is published about five minutes later.
``lavtxt.ascii`` holds forecast hours 1-25 and ``lavtxt_ext.ascii`` hours 26-38.
NOMADS keeps roughly two days, so these must be archived as they appear.
"""

from __future__ import annotations

import datetime as dt
import logging
import sqlite3
import time
from dataclasses import dataclass

import httpx

from ..util.timeutil import UTC, iso, utcnow
from .guidance import Block, extract_blocks, fixed_fields, have_cycle, store_blocks, to_int

log = logging.getLogger(__name__)

NOMADS = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/lmp/prod"
FILES = {"lav": "lavtxt.ascii", "lavx": "lavtxt_ext.ascii"}
_FIRST = 5  # column of the first value field


@dataclass(frozen=True)
class LampRow:
    station: str
    cycle: dt.datetime
    valid: dt.datetime
    fhr: int
    tmp_f: int | None
    dpt_f: int | None


def cycle_url(cycle: dt.datetime, product: str) -> str:
    return f"{NOMADS}/lmp.{cycle:%Y%m%d}/lmp.t{cycle:%H%M}z.{FILES[product]}"


def parse_lav(block: Block) -> list[LampRow]:
    """Hourly temperature and dew point from one LAV (or extended LAV) block.

    Fields are 3 characters wide starting at column 5. The UTC row lists the valid
    hour of each column; the first column is the first whole hour after the cycle,
    or forecast hour 26 in the extended bulletin, which says so in an HR row.
    """
    lines = block.text.splitlines()
    rows = {l[1:4]: l for l in lines if len(l) > 4}
    utc = rows.get("UTC")
    if utc is None or "TMP" not in rows:
        return []
    n = (len(utc.rstrip()) - _FIRST + 2) // 3
    hours = [to_int(x) for x in fixed_fields(utc, _FIRST, 3, n)]
    tmp = [to_int(x) for x in fixed_fields(rows["TMP"], _FIRST, 3, n)]
    dpt = [to_int(x) for x in fixed_fields(rows["DPT"], _FIRST, 3, n)] if "DPT" in rows else [None] * n
    if "HR " in rows:
        fhrs = [to_int(x) for x in fixed_fields(rows["HR "], _FIRST, 3, n)]
    else:
        fhrs = list(range(1, n + 1))
    base = block.cycle.replace(minute=0, second=0, microsecond=0)
    out = []
    for k in range(n):
        if fhrs[k] is None or hours[k] is None:
            continue
        valid = base + dt.timedelta(hours=fhrs[k])
        if valid.hour != hours[k]:
            log.warning("lamp %s %s: column %d hour mismatch", block.station, iso(block.cycle), k)
            return []
        out.append(LampRow(block.station, block.cycle, valid, fhrs[k], tmp[k], dpt[k]))
    return out


class Throttled(Exception):
    """NOMADS answered with its rate-limit redirect; stop and try again later."""


def archive_cycle(conn: sqlite3.Connection, client: httpx.Client, cycle: dt.datetime, wanted: set[str]) -> int | None:
    """Fetch and store both bulletins of one cycle. None if not published (or gone)."""
    total = 0
    for product in FILES:
        r = client.get(cycle_url(cycle, product), follow_redirects=False)
        if r.status_code in (302, 429, 503):
            raise Throttled(f"NOMADS returned {r.status_code}")
        if r.status_code != 200:
            return None if product == "lav" else total
        blocks = [b for b in extract_blocks(r.content, wanted) if b.cycle == cycle]
        conn.execute("BEGIN")
        try:
            total += store_blocks(conn, product, blocks, "nomads", r.headers.get("last-modified"))
            for b in blocks:
                for row in parse_lav(b):
                    conn.execute(
                        """INSERT OR IGNORE INTO lamp_hourly(station, cycle, valid_utc, fhr, tmp_f, dpt_f)
                           VALUES(?,?,?,?,?,?)""",
                        (row.station, iso(row.cycle), iso(row.valid), row.fhr, row.tmp_f, row.dpt_f),
                    )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        missing = wanted - {b.station for b in blocks}
        if missing and product == "lav":
            log.warning("lamp %s: stations missing from bulletin: %s", iso(cycle), sorted(missing))
    return total


def archive_recent(
    conn: sqlite3.Connection,
    client: httpx.Client,
    wanted: set[str],
    *,
    lookback_hours: int = 40,
    now: dt.datetime | None = None,
    pause_s: float = 2.0,
) -> int:
    """Catch up on every hourly cycle of the last ``lookback_hours`` not yet archived.

    Newest first, so that after an outage the cycles about to expire from NOMADS'
    two-day window are not what gets sacrificed to its rate limit.
    """
    now = now or utcnow()
    total = 0
    t = (now - dt.timedelta(minutes=4)).astimezone(UTC).replace(second=0, microsecond=0)
    t = t.replace(minute=30) if t.minute >= 30 else t.replace(minute=30) - dt.timedelta(hours=1)
    oldest = now - dt.timedelta(hours=lookback_hours)
    while t >= oldest:
        if not have_cycle(conn, "lav", t, wanted):
            try:
                total += archive_cycle(conn, client, t, wanted) or 0
            except Throttled as e:
                log.warning("lamp catch-up stopped at %s: %s", iso(t), e)
                break
            except httpx.HTTPError as e:
                log.warning("lamp fetch failed for %s: %s", iso(t), e)
            time.sleep(pause_s)
        t -= dt.timedelta(hours=1)
    return total
