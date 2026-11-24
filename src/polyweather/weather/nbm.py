"""NOAA National Blend of Models station bulletins.

NBP (probabilistic) carries the max/min temperature percentiles used by model M0. It
is issued for the 01, 07, 13 and 19 UTC cycles and appears about 65-80 minutes after
cycle time. Only the 01 and 07 UTC issues still contain the same-day maximum for
CONUS; the 13 and 19 UTC issues start with the next morning's minimum.

Live files come from NOMADS (kept ~2 days); the same files are archived permanently
in the public bucket ``noaa-nbm-grib2-pds`` (back to 2020), a minute or two later.
NBS and NBH (deterministic, hourly cycles) are archived verbatim alongside, unparsed.
"""

from __future__ import annotations

import datetime as dt
import logging
import sqlite3
from dataclasses import dataclass

import httpx

from ..util.http import request_with_retry
from ..util.timeutil import UTC, iso, utcnow
from .guidance import Block, extract_blocks, fixed_fields, have_cycle, store_blocks, to_int

log = logging.getLogger(__name__)

NOMADS = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod"
S3 = "https://noaa-nbm-grib2-pds.s3.amazonaws.com"
FILES = {"nbp": "blend_nbptx", "nbs": "blend_nbstx", "nbh": "blend_nbhtx", "nbe": "blend_nbetx"}
NBP_CYCLES = (1, 7, 13, 19)
_ROWS = {"TXNMN": "mean", "TXNSD": "sd", "TXNP1": "p10", "TXNP2": "p25", "TXNP5": "p50", "TXNP7": "p75", "TXNP9": "p90"}


@dataclass(frozen=True)
class TxnRow:
    station: str
    cycle: dt.datetime
    valid: dt.datetime
    fhr: int
    kind: str  # 'max' for the column valid at 00Z, 'min' for 12Z
    mean: int | None
    sd: int | None
    p10: int | None
    p25: int | None
    p50: int | None
    p75: int | None
    p90: int | None


def cycle_url(base: str, cycle: dt.datetime, product: str) -> str:
    return f"{base}/blend.{cycle:%Y%m%d}/{cycle:%H}/text/{FILES[product]}.t{cycle:%H}z"


def parse_nbp(block: Block) -> list[TxnRow]:
    """Max/min temperature rows of one NBP station block.

    Values sit on a fixed grid: 3-character fields starting at column 7, one
    separator column (space or '|') between fields.
    """
    lines = block.text.splitlines()
    fhr_line = next((l for l in lines if l.startswith(" FHR")), None)
    if fhr_line is None:
        return []
    n = (len(fhr_line.rstrip()) - 7 + 3) // 4
    fhrs = [to_int(x) for x in fixed_fields(fhr_line, 7, 4, n)]
    values: dict[str, list[int | None]] = {}
    for l in lines:
        label = l[1:6].strip()
        if label in _ROWS:
            values[_ROWS[label]] = [to_int(x) for x in fixed_fields(l, 7, 4, n)]
    if "p50" not in values:
        return []
    out = []
    for k, fhr in enumerate(fhrs):
        if fhr is None:
            continue
        valid = block.cycle + dt.timedelta(hours=fhr)
        if valid.hour not in (0, 12):
            continue
        get = lambda name: (values.get(name) or [None] * n)[k]  # noqa: E731
        out.append(
            TxnRow(
                block.station, block.cycle, valid, fhr, "max" if valid.hour == 0 else "min",
                get("mean"), get("sd"), get("p10"), get("p25"), get("p50"), get("p75"), get("p90"),
            )
        )
    return out


def parse_nbs_txn(block: Block) -> list[tuple[dt.datetime, int, str, int]]:
    """Deterministic max/min temperature of one NBS block: (valid, forecast hour, kind, °F).

    NBS is issued every hour, so its latest value is hours fresher than NBP's. Fields
    are 3 characters wide starting at column 5; a TXN value sits under the 00Z column
    (daytime maximum) or the 12Z column (overnight minimum).
    """
    rows = {l[1:4]: l for l in block.text.splitlines() if len(l) > 4}
    if "FHR" not in rows or "TXN" not in rows:
        return []
    n = (len(rows["FHR"].rstrip()) - 5 + 2) // 3
    fhrs = [to_int(x) for x in fixed_fields(rows["FHR"], 5, 3, n)]
    txn = [to_int(x) for x in fixed_fields(rows["TXN"], 5, 3, n)]
    out = []
    for fhr, value in zip(fhrs, txn):
        if fhr is None or value is None:
            continue
        valid = block.cycle + dt.timedelta(hours=fhr)
        if valid.hour in (0, 12):
            out.append((valid, fhr, "max" if valid.hour == 0 else "min", value))
    return out


def store_txn(conn: sqlite3.Connection, rows: list[TxnRow]) -> int:
    new = 0
    for r in rows:
        cur = conn.execute(
            """INSERT OR IGNORE INTO nbm_txn(station, cycle, valid_utc, fhr, kind, mean, sd, p10, p25, p50, p75, p90)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (r.station, iso(r.cycle), iso(r.valid), r.fhr, r.kind, r.mean, r.sd, r.p10, r.p25, r.p50, r.p75, r.p90),
        )
        new += cur.rowcount
    return new


def fetch_cycle(
    client: httpx.Client, cycle: dt.datetime, product: str, wanted: set[str], bases: tuple[str, ...]
) -> tuple[list[Block], str | None, str] | None:
    """Download one cycle's bulletin and cut out the wanted stations.

    Returns (blocks, Last-Modified, source) or None when no base has the file yet.
    """
    for base in bases:
        url = cycle_url(base, cycle, product)
        try:
            if base == NOMADS:
                # No retries or redirects: NOMADS signals its rate limit with a 302,
                # and the S3 copy is the fallback.
                r = client.get(url, follow_redirects=False)
            else:
                r = request_with_retry(client, "GET", url, ok=(200, 403, 404), retries=2)
        except httpx.HTTPError as e:
            log.warning("nbm fetch failed %s: %s", url, e)
            continue
        if r.status_code != 200:
            continue
        blocks = [b for b in extract_blocks(r.content, wanted) if b.cycle == cycle]
        return blocks, r.headers.get("last-modified"), "nomads" if base == NOMADS else "s3"
    return None


def archive_cycle(
    conn: sqlite3.Connection,
    client: httpx.Client,
    cycle: dt.datetime,
    product: str,
    wanted: set[str],
    bases: tuple[str, ...] = (NOMADS, S3),
) -> int | None:
    """Fetch and store one cycle. Returns new block count, or None if not published yet."""
    got = fetch_cycle(client, cycle, product, wanted, bases)
    if got is None:
        return None
    blocks, published, source = got
    conn.execute("BEGIN")
    try:
        new = store_blocks(conn, product, blocks, source, published)
        if product == "nbp":
            for b in blocks:
                store_txn(conn, parse_nbp(b))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    missing = wanted - {b.station for b in blocks}
    if missing:
        log.warning("nbm %s %s: stations missing from bulletin: %s", product, iso(cycle), sorted(missing))
    return new


def expected_cycles(product: str, start: dt.datetime, end: dt.datetime) -> list[dt.datetime]:
    """Cycle times in [start, end] for a product."""
    out = []
    t = start.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    while t <= end:
        if product != "nbp" or t.hour in NBP_CYCLES:
            out.append(t)
        t += dt.timedelta(hours=1)
    return out


def archive_recent(
    conn: sqlite3.Connection,
    client: httpx.Client,
    wanted: set[str],
    *,
    products: tuple[str, ...] = ("nbp", "nbs", "nbh"),
    lookback_hours: int = 30,
    now: dt.datetime | None = None,
) -> dict[str, int]:
    """Catch up on every cycle of the last ``lookback_hours`` not yet archived."""
    now = now or utcnow()
    done: dict[str, int] = {}
    for product in products:
        for cycle in reversed(expected_cycles(product, now - dt.timedelta(hours=lookback_hours), now)):
            if now - cycle < dt.timedelta(minutes=25) or have_cycle(conn, product, cycle, wanted):
                continue
            # Fresh cycles come from NOMADS; anything older is in the S3 archive, which
            # has no rate limit worth worrying about.
            bases = (NOMADS, S3) if now - cycle < dt.timedelta(hours=3) else (S3, NOMADS)
            new = archive_cycle(conn, client, cycle, product, wanted, bases=bases)
            if new:
                done[product] = done.get(product, 0) + new
    return done


def backfill(
    conn: sqlite3.Connection,
    client: httpx.Client,
    wanted: set[str],
    start: dt.date,
    end: dt.date,
    *,
    product: str = "nbp",
    cycles: tuple[int, ...] = NBP_CYCLES,
) -> int:
    """Archive history from the S3 bucket for UTC dates in [start, end)."""
    total = 0
    day = start
    while day < end:
        for hour in cycles:
            cycle = dt.datetime(day.year, day.month, day.day, hour, tzinfo=UTC)
            if have_cycle(conn, product, cycle, wanted):
                continue
            new = archive_cycle(conn, client, cycle, product, wanted, bases=(S3,))
            if new is None:
                log.warning("nbm backfill: %s %s not in archive", product, iso(cycle))
            else:
                total += new
        day += dt.timedelta(days=1)
    return total
