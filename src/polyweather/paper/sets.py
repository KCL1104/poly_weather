"""Do the brackets of one city-day ever add up to a sure profit?

Exactly one bracket of a city-day wins. So:

* one YES share of every bracket pays exactly 1. This needs every bracket: with one
  missing, the missing one may be the winner.
* one NO share of each of k brackets pays at least k - 1, because at most one of them
  loses. This works for any k of them, so the brackets whose NO nobody sells (those
  with no bid on YES, typically the ones already out of reach) are simply left out.

If such a set can be bought for less than it must pay, fees included, that is a sure
profit that needs no forecast at all (spec, second stage: "同組區間的定價一致性", to be
counted from the first day).

This is a measurement, not a ledger. Every recorder snapshot (one a minute) is
checked at the best ask of every leg, where the recording has no hole.
"""

from __future__ import annotations

import json
import sqlite3

import pyarrow.parquet as pq

from .books import BookHistory
from .fills import cost_per_share
from .ledger import ASSUMED_FEE_RATE

CURSOR_KEY = "paper_sets_cursor_ms"


def _events(conn: sqlite3.Connection) -> dict[str, dict]:
    """Open, parsed city-days with all their markets (both tokens known)."""
    out: dict[str, dict] = {}
    rows = conn.execute(
        """SELECT m.event_id, m.market_id, m.yes_token, m.no_token, m.fee_rate, s.station, s.obs_date, s.measure
           FROM markets m JOIN resolution_specs s ON s.event_id = m.event_id JOIN events e ON e.event_id = m.event_id
           WHERE s.status = 'ok' AND m.bracket_ok = 1 AND m.yes_token IS NOT NULL AND m.no_token IS NOT NULL
           ORDER BY m.event_id, m.group_index"""
    ).fetchall()
    for r in rows:
        ev = out.setdefault(r["event_id"], {"station": r["station"], "obs_date": r["obs_date"], "measure": r["measure"], "legs": []})
        ev["legs"].append((r["yes_token"], r["no_token"], ASSUMED_FEE_RATE if r["fee_rate"] is None else float(r["fee_rate"])))
    return out


def scan(conn: sqlite3.Connection, books: BookHistory, until_ms: int) -> dict:
    """Check every snapshot newer than the cursor and up to ``until_ms``."""
    row = conn.execute("SELECT value FROM kv WHERE key=?", (CURSOR_KEY,)).fetchone()
    cursor = int(row[0]) if row else 0
    events = _events(conn)
    checked = found = 0
    last = cursor
    for snap_ms, path in books._snaps:
        if snap_ms <= cursor or snap_ms > until_ms:
            continue
        last = snap_ms
        if books._gap_between(snap_ms - 60_000, snap_ms, "-"):
            continue  # a snapshot taken across a hole may hold stale books
        asks = {}
        for r in pq.read_table(path, columns=["asset_id", "asks"]).to_pylist():
            levels = json.loads(r["asks"])
            if levels:
                asks[r["asset_id"]] = (float(levels[0][0]), float(levels[0][1]))
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT value FROM kv WHERE key=?", (CURSOR_KEY,)).fetchone()
        if row and int(row[0]) >= snap_ms:
            conn.execute("ROLLBACK")  # another run counted this snapshot meanwhile
            continue
        for event_id, ev in events.items():
            yes = [(l[0], l[2]) for l in ev["legs"]]
            no = [(l[1], l[2]) for l in ev["legs"] if l[1] in asks]
            for side, legs, pays in (("YES", yes, 1.0), ("NO", no, len(no) - 1.0)):
                if len(legs) < 2 or any(t not in asks for t, _ in legs):
                    continue
                checked += 1
                n, tokens = len(legs), [t for t, _ in legs]
                cost = sum(cost_per_share(asks[t][0], rate) for t, rate in legs)
                conn.execute(
                    """INSERT INTO paper_set_days(obs_date, side, snapshots, profitable, best_edge) VALUES(?,?,1,?,?)
                       ON CONFLICT(obs_date, side) DO UPDATE SET snapshots = snapshots + 1, profitable = profitable + excluded.profitable,
                           best_edge = MAX(best_edge, excluded.best_edge)""",
                    (ev["obs_date"], side, int(cost < pays), pays - cost),
                )
                if cost < pays:
                    found += 1
                    sets = min(asks[t][1] for t in tokens)
                    conn.execute(
                        """INSERT OR REPLACE INTO paper_set_checks(snap_ms, event_id, station, obs_date, measure, side, legs, cost, edge,
                               sets_at_best, usd_at_best) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                        (snap_ms, event_id, ev["station"], ev["obs_date"], ev["measure"], side, n, cost, pays - cost, sets, sets * cost),
                    )
        conn.execute("INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (CURSOR_KEY, str(last)))
        conn.execute("COMMIT")
    return {"sets_checked": checked, "sets_profitable": found}


def summary(conn: sqlite3.Connection) -> dict:
    out: dict = {}
    for r in conn.execute("SELECT side, SUM(snapshots), SUM(profitable), MAX(best_edge) FROM paper_set_days GROUP BY side"):
        hits = conn.execute(
            "SELECT COUNT(DISTINCT event_id), MAX(edge), AVG(edge), AVG(usd_at_best) FROM paper_set_checks WHERE side=?", (r[0],)
        ).fetchone()
        out[r[0]] = {"city_day_snapshots_checked": r[1], "with_a_sure_profit": r[2], "closest_edge": round(r[3], 4) if r[3] is not None else None,
                     "city_days_involved": hits[0], "largest_edge": None if hits[1] is None else round(hits[1], 4),
                     "mean_edge_when_profitable": None if hits[2] is None else round(hits[2], 4),
                     "mean_usd_buyable_at_best_prices": None if hits[3] is None else round(hits[3], 2)}
    return out
