"""The rules every paper ledger shares, and the one place where signals become fills.

A ledger looks at a city-day at a moment in time with a list of candidates: tokens it
would buy, each with the probability the ledger gives to that token paying 1. From
there the steps are the same for every ledger (plan, "進出場規則"):

1. edge: probability minus what a share costs at the best ask, fee included;
2. filter: market parsed and not frozen, ledger not paused, no hole in the recording,
   at least 5 USD buyable within the limit price;
3. choose: only the candidate with the largest edge is entered at one moment;
4. size: fixed amount or quarter Kelly, then the caps per trade, per city-day and per
   observation date, and a quarter of what each price level shows.

Every candidate leaves a row in ``paper_signals``, entered or not, with the reason.
Fills are simulated twice: conservatively (the numbers that count) and optimistically.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from dataclasses import dataclass, field

from ..catalog.brackets import Bracket
from ..resolution.spec import ResolutionSpec
from ..util.timeutil import UTC, parse_iso
from . import fills
from .books import BookHistory, asks_of, bids_of

SKIP_REASONS = ("below_threshold", "over_cap", "no_liquidity", "frozen", "data_gap", "not_best")
MIN_ORDER_USD = 1.0


@dataclass(frozen=True)
class LedgerRules:
    name: str
    model_version: str
    min_edge: float
    max_trade_usd: float
    kelly_fraction: float | None = None  # None: always the fixed max_trade_usd
    max_price: dict[str, float] = field(default_factory=dict)  # tail protection, e.g. {"NO": 0.97}
    bankroll_usd: float = 2000.0
    city_day_cap_usd: float = 60.0
    day_cap_usd: float = 300.0


@dataclass
class CityDay:
    event_id: str
    station: str
    obs_date: str
    spec: ResolutionSpec
    markets: list[tuple[sqlite3.Row, Bracket]]
    day_start: dt.datetime
    day_end: dt.datetime


@dataclass
class Candidate:
    market: sqlite3.Row
    side: str  # 'YES' / 'NO'
    win_prob: float
    detail: dict = field(default_factory=dict)

    @property
    def token(self) -> str:
        return self.market["yes_token"] if self.side == "YES" else self.market["no_token"]


def _ms(iso_text: str | None) -> int | None:
    return int(parse_iso(iso_text).timestamp() * 1000) if iso_text else None


def open_city_day(conn: sqlite3.Connection, station: str, when: dt.datetime, ts_ms: int, measure: str = "high") -> CityDay | None:
    """The station's market for the local day that contains ``when``, if it was open at ``ts_ms``."""
    rows = conn.execute(
        """SELECT e.event_id, e.closed_time, s.spec_json FROM resolution_specs s JOIN events e ON e.event_id = s.event_id
           WHERE s.status = 'ok' AND s.station = ? AND s.measure = ? AND s.obs_date BETWEEN ? AND ?""",
        (station, measure, (when - dt.timedelta(days=1)).strftime("%Y-%m-%d"), (when + dt.timedelta(days=1)).strftime("%Y-%m-%d")),
    ).fetchall()
    for r in rows:
        spec = ResolutionSpec.from_json(r["spec_json"])
        start, end = spec.local_day_bounds_utc()
        if not (start <= when < end):
            continue
        closed = _ms(r["closed_time"])
        if closed is not None and closed <= ts_ms:
            return None
        markets = conn.execute("SELECT * FROM markets WHERE event_id=? ORDER BY group_index", (r["event_id"],)).fetchall()
        usable = [(m, Bracket(m["lo"], m["hi"], m["unit"])) for m in markets if m["bracket_ok"] and m["yes_token"] and m["no_token"]]
        if not usable:
            return None
        return CityDay(r["event_id"], station, spec.obs_date or "", spec, usable, start, end)
    return None


def paused_reason(conn: sqlite3.Connection, ts_ms: int, station: str, ledger: str) -> str | None:
    """Why this ledger may not open positions for this station at ``ts_ms``, per the pause log."""
    for scope in (f"station:{station}", f"ledger:{ledger}", "all"):
        row = conn.execute(
            "SELECT action, trigger FROM pause_log WHERE scope=? AND ts_ms<=? ORDER BY ts_ms DESC, id DESC LIMIT 1", (scope, ts_ms)
        ).fetchone()
        if row and row["action"] == "pause":
            return f"{scope} paused: {row['trigger']}"
    return None


def spent(conn: sqlite3.Connection, run: str, ledger: str, *, event_id: str | None = None, obs_date: str | None = None) -> float:
    """USD the ledger has put into a city-day or an observation date, fees included (conservative fills)."""
    q = "SELECT COALESCE(SUM(notional + fee), 0) FROM paper_fills WHERE run=? AND ledger=? AND variant='conservative'"
    args: list = [run, ledger]
    if event_id is not None:
        q += " AND event_id=?"
        args.append(event_id)
    if obs_date is not None:
        q += " AND obs_date=?"
        args.append(obs_date)
    return float(conn.execute(q, args).fetchone()[0])


def _quote(book) -> dict:
    bids, asks = bids_of(book), asks_of(book)
    bb, ba = (bids[0] if bids else (None, None)), (asks[0] if asks else (None, None))
    mid = (bb[0] + ba[0]) / 2 if bb[0] is not None and ba[0] is not None else None
    return {"best_bid": bb[0], "best_bid_size": bb[1], "best_ask": ba[0], "best_ask_size": ba[1], "market_mid": mid}


def decide(
    conn: sqlite3.Connection,
    books: BookHistory,
    rules: LedgerRules,
    run: str,
    ts_ms: int,
    city_day: CityDay,
    candidates: list[Candidate],
    source_batch: str,
) -> dict:
    """Run one decision for one city-day; record every candidate; return counts."""
    counts = {"candidates": len(candidates), "entered": 0}
    if not candidates:
        return counts
    tokens = [c.token for c in candidates]
    at_signal = books.at(ts_ms, tokens)
    at_fill = books.at(ts_ms + fills.LATENCY_MS, tokens)
    why_gap = dict(books.why)
    blocked = paused_reason(conn, ts_ms, city_day.station, rules.name)
    left = min(
        rules.city_day_cap_usd - spent(conn, run, rules.name, event_id=city_day.event_id),
        rules.day_cap_usd - spent(conn, run, rules.name, obs_date=city_day.obs_date),
    )

    rows = []
    for c in candidates:
        rate = float(c.market["fee_rate"] or 0.0)
        tick = float(c.market["tick_size"] or 0.001)
        row = {"c": c, "rate": rate, "quote": {}, "edge": None, "reason": None, "detail": dict(c.detail), "fill": None, "fill_opt": None}
        rows.append(row)
        book_now, book_later = at_signal.get(c.token), at_fill.get(c.token)
        if book_now is not None:
            row["quote"] = _quote(book_now)
            if row["quote"]["best_ask"] is not None:
                row["edge"] = c.win_prob - fills.cost_per_share(row["quote"]["best_ask"], rate)
        if blocked:
            row["reason"], row["detail"]["why"] = "frozen", blocked
            continue
        if book_now is None or book_later is None:
            row["reason"], row["detail"]["why"] = "data_gap", why_gap.get(c.token, "no book")
            continue
        limit = fills.limit_price(c.win_prob, rules.min_edge, rate, tick)
        cap = rules.max_price.get(c.side)
        if cap is not None and limit is not None and limit > cap:
            limit, row["detail"]["tail_protection"] = cap, cap
        asks = asks_of(book_later)
        if limit is None or not asks or asks[0][0] > limit + 1e-12:
            row["reason"] = "below_threshold"
            row["detail"]["limit"] = limit
            continue
        if rules.kelly_fraction is None:
            want = rules.max_trade_usd
        else:
            cost = fills.cost_per_share(asks[0][0], rate)
            kelly = max(0.0, (c.win_prob - cost) / (1.0 - cost)) if cost < 1 else 0.0
            want = min(rules.max_trade_usd, rules.bankroll_usd * rules.kelly_fraction * kelly)
        budget = min(want, left)
        row["detail"].update(limit=limit, wanted_usd=round(want, 2), cap_left_usd=round(left, 2))
        probe = fills.take(asks, limit=limit, budget_usd=max(budget, 0.0), rate=rate)
        row["detail"]["fillable_usd"] = round(probe.fillable_usd, 2)
        if probe.fillable_usd < fills.MIN_FILLABLE_USD:
            row["reason"] = "no_liquidity"
            continue
        if budget < MIN_ORDER_USD:
            row["reason"] = "over_cap"
            continue
        min_shares = float(c.market["min_order_size"] or 0.0)
        if probe.shares <= 0 or probe.shares < min_shares:
            row["reason"] = "no_liquidity"
            row["detail"]["min_order_size"] = min_shares
            continue
        row["fill"] = probe
        row["fill_opt"] = fills.take(asks_of(book_now), limit=limit, budget_usd=budget, rate=rate, level_share=1.0)

    passing = [r for r in rows if r["reason"] is None]
    best = max(passing, key=lambda r: (r["c"].win_prob - r["fill"].avg_cost, -r["c"].market["group_index"])) if passing else None
    for r in passing:
        if r is not best:
            r["reason"] = "not_best"

    for r in rows:
        c, q = r["c"], r["quote"]
        entered = r is best
        cur = conn.execute(
            """INSERT OR REPLACE INTO paper_signals(run, ledger, ts_ms, event_id, market_id, station, obs_date, side, model_prob,
                   market_mid, best_bid, best_bid_size, best_ask, best_ask_size, edge, decision, skip_reason, model_version,
                   source_batch, detail)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (run, rules.name, ts_ms, city_day.event_id, c.market["market_id"], city_day.station, city_day.obs_date, c.side,
             c.win_prob, q.get("market_mid"), q.get("best_bid"), q.get("best_bid_size"), q.get("best_ask"), q.get("best_ask_size"),
             r["edge"], "enter" if entered else "skip", None if entered else r["reason"], rules.model_version, source_batch,
             json.dumps(r["detail"], sort_keys=True, default=str)),
        )
        if not entered:
            continue
        counts["entered"] += 1
        for variant, fill, when in (("conservative", r["fill"], ts_ms + fills.LATENCY_MS), ("optimistic", r["fill_opt"], ts_ms)):
            if fill is None or fill.shares <= 0:
                continue
            conn.execute(
                """INSERT OR REPLACE INTO paper_fills(signal_id, run, ledger, variant, fill_ms, event_id, market_id, station, obs_date,
                       side, shares, avg_price, notional, fee, levels)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (cur.lastrowid, run, rules.name, variant, when, city_day.event_id, c.market["market_id"], city_day.station,
                 city_day.obs_date, c.side, fill.shares, fill.avg_price, fill.notional, fill.fee, json.dumps(fill.levels)),
            )
    return counts


def settle(conn: sqlite3.Connection, now_ms: int) -> int:
    """Pay out fills whose market has resolved: one USD per share if the token won. By the
    market's actual resolution, never by this system's own reconstruction."""
    rows = conn.execute(
        """SELECT f.id, f.side, f.shares, m.winner FROM paper_fills f JOIN markets m ON m.market_id = f.market_id
           WHERE f.payout IS NULL AND m.winner IS NOT NULL"""
    ).fetchall()
    for r in rows:
        conn.execute("UPDATE paper_fills SET payout=?, settled_ms=? WHERE id=?",
                     (r["shares"] if r["winner"] == r["side"] else 0.0, now_ms, r["id"]))
    return len(rows)


def utc_iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
