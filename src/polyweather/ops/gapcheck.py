"""Daily archive completeness check (the "no gaps for 7 days" gate).

For one UTC date it asks, per archive: did everything that should exist arrive, and
was our own collection running the whole time? Each answer is stored in
``daily_checks``. Reports missing at the source (a station that skipped an hour) are
listed but do not fail the check; failures of our own collection do.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from dataclasses import dataclass

from .. import stations as station_registry
from ..settings import Settings
from ..storage.parquet_sink import read_partitions
from ..util.timeutil import UTC, iso, now_ms
from ..weather import nbm
from ..weather.guidance import have_cycle

BOOK_GAP_LIMIT_MS = 60_000


@dataclass
class Check:
    name: str
    ok: bool
    detail: dict


def _day_bounds(day: dt.date) -> tuple[dt.datetime, dt.datetime]:
    start = dt.datetime(day.year, day.month, day.day, tzinfo=UTC)
    return start, start + dt.timedelta(days=1)


def _ms(d: dt.datetime) -> int:
    return int(d.timestamp() * 1000)


def check_book(conn: sqlite3.Connection, settings: Settings, day: dt.date) -> Check:
    start, end = _day_bounds(day)
    rows = conn.execute(
        "SELECT start_ms, end_ms, key, reason FROM gaps WHERE source='book' AND start_ms < ? AND (end_ms IS NULL OR end_ms > ?)",
        (_ms(end), _ms(start)),
    ).fetchall()
    long_gaps = []
    total = 0
    for r in rows:
        a = max(r["start_ms"], _ms(start))
        b = min(r["end_ms"] if r["end_ms"] is not None else now_ms(), _ms(end))
        total += max(0, b - a)
        if b - a > BOOK_GAP_LIMIT_MS:
            long_gaps.append({"start": iso(dt.datetime.fromtimestamp(a / 1000, UTC)), "seconds": round((b - a) / 1000), "key": r["key"], "reason": r["reason"]})
    raw_files = read_partitions(settings.raw_dir / "book", day.isoformat(), day.isoformat())
    snap_files = read_partitions(settings.raw_dir / "snapshots", day.isoformat(), day.isoformat())
    ok = bool(raw_files) and not long_gaps and len(snap_files) >= 1380  # 1440 expected; allow 60 late/skipped
    return Check(
        "book",
        ok,
        {"raw_files": len(raw_files), "snapshots": len(snap_files), "gaps": len(rows),
         "gap_seconds": round(total / 1000), "gaps_over_60s": long_gaps},
    )


def check_metar(conn: sqlite3.Connection, settings: Settings, day: dt.date) -> Check:
    start, end = _day_bounds(day)
    poll_gaps = conn.execute(
        "SELECT COUNT(*) FROM gaps WHERE source='metar' AND start_ms < ? AND (end_ms IS NULL OR end_ms > ?)",
        (_ms(end), _ms(start)),
    ).fetchone()[0]
    missing: dict[str, list[int]] = {}
    for st in station_registry.in_scope(settings.scope):
        hours = {
            int(r["obs_time"][11:13])
            for r in conn.execute(
                "SELECT obs_time FROM metar_obs WHERE station=? AND obs_time>=? AND obs_time<? "
                "AND source LIKE 'awc%' AND COALESCE(metar_type,'METAR') != 'SPECI' AND temp_tenths_c IS NOT NULL",
                (st.icao, iso(start), iso(end)),
            )
        }
        gone = sorted(set(range(24)) - hours)
        if gone:
            missing[st.icao] = gone
    dead = [s for s, h in missing.items() if len(h) > 6]
    return Check("metar", poll_gaps == 0 and not dead, {"poll_gaps": poll_gaps, "hours_missing_at_source": missing, "stations_mostly_missing": dead})


def check_nbm(conn: sqlite3.Connection, settings: Settings, day: dt.date) -> Check:
    start, end = _day_bounds(day)
    wanted = {s.icao for s in station_registry.in_scope(settings.scope) if s.us}
    if not wanted:
        return Check("nbm", True, {"note": "no US stations in scope"})
    missing = {}
    for product in ("nbp", "nbs", "nbh"):
        gone = [iso(c) for c in nbm.expected_cycles(product, start, end - dt.timedelta(hours=1)) if not have_cycle(conn, product, c, wanted)]
        if gone:
            missing[product] = gone
    return Check("nbm", "nbp" not in missing and len(missing.get("nbh", [])) <= 2 and len(missing.get("nbs", [])) <= 2, {"missing_cycles": missing})


def check_lamp(conn: sqlite3.Connection, settings: Settings, day: dt.date) -> Check:
    start, _ = _day_bounds(day)
    wanted = {s.icao for s in station_registry.in_scope(settings.scope) if s.us}
    if not wanted:
        return Check("lamp", True, {"note": "no US stations in scope"})
    gone = []
    for h in range(24):
        c = start + dt.timedelta(hours=h, minutes=30)
        if not have_cycle(conn, "lav", c, wanted):
            gone.append(iso(c))
    return Check("lamp", len(gone) <= 1, {"missing_cycles": gone})


def check_ensemble(conn: sqlite3.Connection, settings: Settings, day: dt.date) -> Check:
    start, end = _day_bounds(day)
    rows = conn.execute(
        "SELECT source, model, COUNT(*) AS n FROM forecast_fetches WHERE fetched_ms>=? AND fetched_ms<? GROUP BY 1,2",
        (_ms(start), _ms(end)),
    ).fetchall()
    found = {f"{r['source']}:{r['model']}": r["n"] for r in rows}
    return Check("ensemble", any(n >= 2 for n in found.values()), {"distinct_versions": found})


def check_jobs(conn: sqlite3.Connection, settings: Settings, day: dt.date) -> Check:
    start, end = _day_bounds(day)
    rows = conn.execute(
        "SELECT job, COUNT(*) AS n, SUM(ok) AS ok FROM job_runs WHERE ts_ms>=? AND ts_ms<? GROUP BY job",
        (_ms(start), _ms(end)),
    ).fetchall()
    runs = {r["job"]: {"runs": r["n"], "ok": r["ok"]} for r in rows}
    scans = runs.get("catalog", {}).get("ok", 0)
    return Check("catalog", scans >= 130, {"runs": runs, "expected_catalog_scans": 144})


CHECKS = (check_book, check_metar, check_nbm, check_lamp, check_ensemble, check_jobs)


def run(conn: sqlite3.Connection, settings: Settings, day: dt.date) -> list[Check]:
    out = []
    for fn in CHECKS:
        c = fn(conn, settings, day)
        conn.execute(
            """INSERT INTO daily_checks(date, name, ok, detail, ts_ms) VALUES(?,?,?,?,?)
               ON CONFLICT(date, name) DO UPDATE SET ok=excluded.ok, detail=excluded.detail, ts_ms=excluded.ts_ms""",
            (day.isoformat(), c.name, int(c.ok), json.dumps(c.detail), now_ms()),
        )
        out.append(c)
    return out


def clean_streak(conn: sqlite3.Connection, until: dt.date) -> int:
    """Consecutive days up to ``until`` on which every check passed."""
    n = 0
    day = until
    while True:
        rows = conn.execute("SELECT ok FROM daily_checks WHERE date=?", (day.isoformat(),)).fetchall()
        if len(rows) < len(CHECKS) or not all(r["ok"] for r in rows):
            return n
        n += 1
        day -= dt.timedelta(days=1)
