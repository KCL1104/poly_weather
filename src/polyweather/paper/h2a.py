"""H2a: brackets the day's observations have already ruled out.

The daily high only moves up. Once a counted report shows a temperature above a
bracket's upper bound, that bracket cannot win, and its NO side is worth exactly 1.
The hypothesis is that the market is slow to price this in. The ledger buys NO of
ruled-out brackets when that still leaves 2 percentage points after the fee, 10 USD
at a time (plan, ledger table). No model is involved.

Next to the ledger, every bracket that has just been ruled out is measured: what its
NO side cost, and how much could be bought, at the moment the report was seen and 1,
5 and 15 minutes later (spec, H2 test).
"""

from __future__ import annotations

import sqlite3

from ..resolution.settle import settle
from ..weather.metar_store import load_obs
from . import fills
from .books import BookHistory, asks_of
from .ledger import ASSUMED_FEE_RATE, Candidate, CityDay, LedgerRules

RULES = LedgerRules(name="H2a", model_version="h2a-1", min_edge=0.02, max_trade_usd=10.0)
MEASURE_OFFSETS_S = (0, 60, 300, 900)


def observed_high(conn: sqlite3.Connection, city_day: CityDay, ts_ms: int):
    """The day's qualifying high so far, from the reports this system had seen by ``ts_ms``."""
    obs = load_obs(conn, city_day.station, city_day.day_start, city_day.day_end, seen_until_ms=ts_ms)
    return settle(city_day.spec, obs)


def ruled_out(city_day: CityDay, high_unit: int | None) -> list:
    """Markets whose bracket lies entirely below the observed high."""
    if high_unit is None:
        return []
    return [m for m, bracket in city_day.markets if bracket.hi is not None and bracket.hi < high_unit]


def candidates(conn: sqlite3.Connection, city_day: CityDay, ts_ms: int) -> tuple[list[Candidate], dict]:
    s = observed_high(conn, city_day, ts_ms)
    info = {"observed_high": s.value_unit, "reports_counted": s.n_counted,
            "deciding_obs": s.deciding_time.strftime("%Y-%m-%dT%H:%M:%SZ") if s.deciding_time else None}
    return [Candidate(m, "NO", 1.0, dict(info)) for m in ruled_out(city_day, s.value_unit)], info


def plan_measurements(conn: sqlite3.Connection, run: str, city_day: CityDay, ts_ms: int, trigger_obs: str) -> int:
    """Schedule the measurements for brackets that this very report ruled out."""
    now_out = {m["market_id"]: m for m in ruled_out(city_day, observed_high(conn, city_day, ts_ms).value_unit)}
    earlier = observed_high(conn, city_day, ts_ms - 1)
    before = {m["market_id"] for m in ruled_out(city_day, earlier.value_unit)}
    # The day's first report rules out, all at once, every bracket far below the night's temperature.
    # Those were never in play, so they are marked and reported apart from brackets that fall during the day.
    first_of_day = int(earlier.n_counted == 0)
    n = 0
    for market_id in now_out.keys() - before:
        for offset in MEASURE_OFFSETS_S:
            conn.execute(
                """INSERT OR IGNORE INTO paper_measurements(run, ledger, event_id, market_id, station, obs_date, trigger, seen_ms,
                       offset_s, at_ms, status, first_of_day) VALUES(?,?,?,?,?,?,?,?,?,?,'pending',?)""",
                (run, RULES.name, city_day.event_id, market_id, city_day.station, city_day.obs_date, trigger_obs, ts_ms, offset,
                 ts_ms + offset * 1000 + fills.LATENCY_MS, first_of_day),
            )
            n += 1
    return n


def take_measurements(conn: sqlite3.Connection, books: BookHistory, run: str, horizon_ms: int) -> int:
    """Fill in the measurements whose moment the recording now covers."""
    rows = conn.execute(
        """SELECT p.rowid AS rid, p.market_id, p.at_ms, m.no_token, m.fee_rate FROM paper_measurements p
           JOIN markets m ON m.market_id = p.market_id
           WHERE p.run=? AND p.ledger=? AND p.status='pending' AND p.at_ms<=? ORDER BY p.at_ms""",
        (run, RULES.name, horizon_ms),
    ).fetchall()
    for r in rows:
        book = books.at(r["at_ms"], [r["no_token"]]).get(r["no_token"])
        if book is None:
            conn.execute("UPDATE paper_measurements SET status='gap' WHERE rowid=?", (r["rid"],))
            continue
        asks = [(p, s) for p, s in asks_of(book) if p < 1.0]
        rate = ASSUMED_FEE_RATE if r["fee_rate"] is None else float(r["fee_rate"])
        best = asks[0] if asks else (None, None)
        # The falsification test asks how much could be bought. Counted twice: everything below 1 (the
        # sell orders parked at 0.999 included), and only what still leaves the ledger's threshold.
        limit = fills.limit_price(1.0, RULES.min_edge, rate, fills.FINEST_TICK)
        within = 0.0 if limit is None else sum(p * s * fills.LEVEL_SHARE for p, s in asks if p <= limit + 1e-12)
        conn.execute(
            """UPDATE paper_measurements SET status='ok', best_ask=?, ask_size=?, fillable_usd=?, edge=?, fillable_at_threshold_usd=?
               WHERE rowid=?""",
            (best[0], best[1], sum(p * s * fills.LEVEL_SHARE for p, s in asks),
             None if best[0] is None else 1.0 - fills.cost_per_share(best[0], rate), within, r["rid"]),
        )
    return len(rows)
