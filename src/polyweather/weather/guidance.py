"""Shared pieces for NOAA station text bulletins (NBM and LAMP).

Both come as one large fixed-width text file per model cycle holding a block per
station. Only the blocks of tracked stations are kept, verbatim, so any field can be
re-parsed later without the original file.
"""

from __future__ import annotations

import datetime as dt
import re
import sqlite3
from dataclasses import dataclass

from ..util.timeutil import UTC, iso, now_ms

_HEADER = re.compile(
    rb"^ ?(?P<station>[A-Z0-9]{3,6}) +(?P<model>NBM|GFS LAMP)(?: +V\d+\.\d+)? +(?P<product>[A-Z]{3} )?GUIDANCE"
    rb" +(?P<month>\d{1,2})/(?P<day>\d{2})/(?P<year>\d{4}) +(?P<hhmm>\d{4}) UTC",
    re.M,
)


@dataclass(frozen=True)
class Block:
    station: str
    cycle: dt.datetime
    text: str


def extract_blocks(data: bytes, wanted: set[str]) -> list[Block]:
    """Return the blocks of the wanted stations from a bulletin file."""
    out: list[Block] = []
    for station in sorted(wanted):
        key = station.encode()
        pos = 0
        while True:
            i = data.find(b" " + key + b" ", pos)
            if i < 0:
                break
            line_start = data.rfind(b"\n", 0, i) + 1
            m = _HEADER.match(data, line_start)
            if not m or m.group("station") != key:
                pos = i + 1
                continue
            end = _block_end(data, m.end())
            cycle = dt.datetime(
                int(m.group("year")), int(m.group("month")), int(m.group("day")),
                int(m.group("hhmm")[:2]), int(m.group("hhmm")[2:]), tzinfo=UTC,
            )
            text = data[line_start:end].decode("ascii", "replace")
            out.append(Block(station, cycle, "\n".join(l.rstrip() for l in text.splitlines())))
            break
    return out


def _block_end(data: bytes, start: int) -> int:
    """Offset of the first whitespace-only line after ``start`` (or end of data)."""
    pos = start
    while True:
        nl = data.find(b"\n", pos)
        if nl < 0:
            return len(data)
        nxt = data.find(b"\n", nl + 1)
        line = data[nl + 1 : nxt if nxt >= 0 else len(data)]
        if not line.strip():
            return nl
        pos = nl + 1


def fixed_fields(line: str, first: int, width: int, count: int) -> list[str]:
    """Slice ``count`` right-aligned fields of ``width`` chars starting at column ``first``."""
    return [line[first + k * width : first + (k + 1) * width].replace("|", " ").strip() for k in range(count)]


def to_int(s: str) -> int | None:
    try:
        return int(s)
    except ValueError:
        return None


def store_blocks(
    conn: sqlite3.Connection, product: str, blocks: list[Block], source: str, published: str | None,
    fetched_ms: int | None = None,
) -> int:
    new = 0
    ts = fetched_ms or now_ms()
    for b in blocks:
        cur = conn.execute(
            """INSERT OR IGNORE INTO guidance_blocks(product, station, cycle, raw_block, source, published, fetched_ms)
               VALUES(?,?,?,?,?,?,?)""",
            (product, b.station, iso(b.cycle), b.text, source, published, ts),
        )
        new += cur.rowcount
    return new


def have_cycle(conn: sqlite3.Connection, product: str, cycle: dt.datetime, stations: set[str]) -> bool:
    n = conn.execute(
        f"SELECT COUNT(*) FROM guidance_blocks WHERE product=? AND cycle=? AND station IN ({','.join('?' * len(stations))})",
        (product, iso(cycle), *sorted(stations)),
    ).fetchone()[0]
    return n >= len(stations)
