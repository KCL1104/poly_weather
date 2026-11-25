"""Drive the paper ledgers from what the recorder and the archives have written.

There is no live trading loop. The engine reads the archive a few minutes behind the
present and works through the triggers in the order the system learned of them: a
counted METAR report (H2a), a new LAMP cycle (H2b). Every decision uses only what had
been seen by the trigger's own time, so the same code run later over the same day
gives the same ledger. That is what the daily reconciliation checks: each finished
observation date is recomputed from scratch under its own run label and compared with
the ledger of record; more than 1 USD apart pauses every ledger (plan, "暫停條件").

Observation dates are independent of one another: the caps are per city-day and per
observation date, sizing uses the fixed starting bankroll, and positions are held to
settlement. Pauses are read from the pause log as of the trigger's time.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
from dataclasses import dataclass, field

from .. import stations
from ..settings import Settings
from ..util.timeutil import UTC, now_ms, parse_iso
from . import fills, h2a, h2b, ledger
from .books import BookHistory

log = logging.getLogger(__name__)

LIVE = "live"
LAG_MS = 180_000  # stay this far behind the recording: a lost connection is logged as a gap within a minute
STALE_RECORDER_MS = 180_000
RECONCILE_TOLERANCE_USD = 1.0
DRAWDOWN_SHARE = 0.25
LEDGERS = {h2a.RULES.name: h2a.RULES, h2b.RULES.name: h2b.RULES}


@dataclass
class RunSummary:
    run: str
    from_ms: int = 0
    to_ms: int = 0
    triggers: int = 0
    signals: int = 0
    entered: int = 0
    measurements_planned: int = 0
    measurements_taken: int = 0
    settled: int = 0
    decisions_not_taken: dict[str, int] = field(default_factory=dict)  # whole decisions skipped, by reason
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        d = dict(self.__dict__)
        d["from"], d["to"] = ledger.utc_iso(self.from_ms), ledger.utc_iso(self.to_ms)
        return d


# ---- how far the archive can be trusted ------------------------------------------------


def horizon_ms(settings: Settings, now: int | None = None) -> int:
    """The latest moment the engine may decide about: safely behind what the recorder has on disk."""
    now = now or now_ms()
    try:
        state = json.loads((settings.state_dir / "recorder.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        state = {}
    alive, flushed = state.get("last_alive_ms"), state.get("flushed_through_ms")
    if alive and flushed and now - alive < STALE_RECORDER_MS:
        return min(now, flushed) - LAG_MS
    # Recorder not running: nothing new will be written for the time already past, and
    # what is missing shows up as a hole in the recording when a decision is attempted.
    return now - LAG_MS


# ---- triggers ---------------------------------------------------------------------------


def _us_stations() -> list[str]:
    return [s.icao for s in stations.in_scope("us")]


def metar_triggers(conn: sqlite3.Connection, after_ms: int, until_ms: int) -> list[tuple[int, str, list[sqlite3.Row]]]:
    """(time seen, station, reports) for counted reports first seen in (after, until], oldest first."""
    ids = _us_stations()
    rows = conn.execute(
        f"""SELECT station, obs_time, first_seen_ms, metar_type, temp_tenths_c FROM metar_obs
            WHERE first_seen_ms > ? AND first_seen_ms <= ? AND temp_tenths_c IS NOT NULL
              AND COALESCE(metar_type, 'METAR') != 'SPECI' AND station IN ({','.join('?' * len(ids))})
            ORDER BY first_seen_ms, station, obs_time""",
        (after_ms, until_ms, *ids),
    ).fetchall()
    grouped: dict[tuple[int, str], list[sqlite3.Row]] = {}
    for r in rows:
        grouped.setdefault((r["first_seen_ms"], r["station"]), []).append(r)
    return [(ts, station, reports) for (ts, station), reports in sorted(grouped.items())]


def _handle_metar(conn, books, run: str, ts: int, station: str, reports: list[sqlite3.Row], only_date: str | None, s: RunSummary) -> None:
    seen: set[str] = set()
    for report in reports:
        when = parse_iso(report["obs_time"])
        city_day = ledger.open_city_day(conn, station, when, ts)
        if city_day is None or city_day.event_id in seen:
            continue
        if only_date is not None and city_day.obs_date != only_date:
            continue
        seen.add(city_day.event_id)
        s.triggers += 1
        cands, _info = h2a.candidates(conn, city_day, ts)
        newest = max(r["obs_time"] for r in reports if city_day.day_start <= parse_iso(r["obs_time"]) < city_day.day_end)
        if run == LIVE:  # the measurement is taken once, by the ledger of record
            s.measurements_planned += h2a.plan_measurements(conn, run, city_day, ts, newest)
        counts = ledger.decide(conn, books, h2a.RULES, run, ts, city_day, cands, f"METAR {station} {newest}")
        s.signals += counts["candidates"]
        s.entered += counts["entered"]


def lamp_triggers(conn: sqlite3.Connection, after_ms: int, until_ms: int) -> list[tuple[int, str, dt.datetime]]:
    """(time archived, station, cycle) for LAMP cycles first archived in (after, until]; one per station and moment."""
    ids = _us_stations()
    rows = conn.execute(
        f"""SELECT fetched_ms, station, MAX(cycle) FROM guidance_blocks
            WHERE product='lav' AND fetched_ms > ? AND fetched_ms <= ? AND station IN ({','.join('?' * len(ids))})
            GROUP BY fetched_ms, station ORDER BY fetched_ms, station""",
        (after_ms, until_ms, *ids),
    ).fetchall()
    return [(r[0], r[1], parse_iso(r[2])) for r in rows]


def _handle_lamp(conn, books, run: str, ts: int, station: str, cycle: dt.datetime, only_date: str | None, s: RunSummary) -> None:
    if h2b.newest_cycle(conn, station, ts) != cycle:
        return  # an older cycle fetched in a catch-up: the newer one already had its decision
    city_day = ledger.open_city_day(conn, station, dt.datetime.fromtimestamp(ts / 1000, UTC), ts)
    if city_day is None or (only_date is not None and city_day.obs_date != only_date):
        return
    s.triggers += 1
    cands, info = h2b.candidates(conn, city_day, ts, cycle, run)
    if not cands:
        why = info.get("skipped", "no candidates")
        why = "not enough past days for this cycle hour" if why.startswith("only ") else why
        s.decisions_not_taken[f"H2b: {why}"] = s.decisions_not_taken.get(f"H2b: {why}", 0) + 1
        return
    counts = ledger.decide(conn, books, h2b.RULES, run, ts, city_day, cands, f"LAMP {station} {info['cycle']}")
    s.signals += counts["candidates"]
    s.entered += counts["entered"]


# ---- runs ---------------------------------------------------------------------------------


def _cursor(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT value FROM kv WHERE key='paper_cursor_ms'").fetchone()
    return int(row[0]) if row else None


def _set_cursor(conn: sqlite3.Connection, value: int) -> None:
    conn.execute("INSERT INTO kv(key, value) VALUES('paper_cursor_ms', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(value),))


def process(conn: sqlite3.Connection, settings: Settings, run: str, after_ms: int, until_ms: int,
            only_date: str | None = None, books: BookHistory | None = None) -> RunSummary:
    """Work through every trigger seen in (after_ms, until_ms] for one run label."""
    books = books or BookHistory(settings, conn)
    s = RunSummary(run=run, from_ms=after_ms, to_ms=until_ms)
    triggers = [(ts, 0, station, reports) for ts, station, reports in metar_triggers(conn, after_ms, until_ms)]
    triggers += [(ts, 1, station, cycle) for ts, station, cycle in lamp_triggers(conn, after_ms, until_ms)]
    for ts, kind, station, payload in sorted(triggers, key=lambda t: (t[0], t[1], t[2])):
        conn.execute("BEGIN")
        try:
            if kind == 0:
                _handle_metar(conn, books, run, ts, station, payload, only_date, s)
            else:
                _handle_lamp(conn, books, run, ts, station, payload, only_date, s)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return s


def run_live(conn: sqlite3.Connection, settings: Settings, now: int | None = None) -> RunSummary:
    """Advance the ledger of record to the present (minus the safety lag), settle, check."""
    now = now or now_ms()
    until = horizon_ms(settings, now)
    after = _cursor(conn)
    if after is None:
        # First run: start with the recording, not with history fetched before it began.
        first = BookHistory(settings, conn)._snaps
        after = (first[0][0] - 1) if first else until
    books = BookHistory(settings, conn)
    if until <= after:
        s = RunSummary(run=LIVE, from_ms=after, to_ms=after, notes=["nothing new the recording covers yet"])
    else:
        s = process(conn, settings, LIVE, after, until, books=books)
        _set_cursor(conn, until)
    s.measurements_taken = h2a.take_measurements(conn, books, LIVE, until)
    s.settled = ledger.settle(conn, now)
    s.notes += check_drawdown(conn, now)
    s.notes += reconcile_due(conn, settings, now)
    return s


# ---- reconciliation -------------------------------------------------------------------------


def _date_window(conn: sqlite3.Connection, obs_date: str) -> tuple[int, int] | None:
    """Earliest local-day start and latest market close among this date's US high markets, in ms."""
    rows = conn.execute(
        """SELECT s.spec_json, e.closed_time FROM resolution_specs s JOIN events e ON e.event_id = s.event_id
           WHERE s.status='ok' AND s.measure='high' AND s.obs_date=? AND s.station IN ({})""".format(",".join("?" * len(_us_stations()))),
        (obs_date, *_us_stations()),
    ).fetchall()
    if not rows:
        return None
    from ..resolution.spec import ResolutionSpec

    starts, ends = [], []
    for r in rows:
        a, b = ResolutionSpec.from_json(r["spec_json"]).local_day_bounds_utc()
        starts.append(int(a.timestamp() * 1000))
        close = parse_iso(r["closed_time"]).timestamp() * 1000 if r["closed_time"] else b.timestamp() * 1000 + 12 * 3600_000
        ends.append(int(max(b.timestamp() * 1000, close)))
    return min(starts), max(ends)


def replay_date(conn: sqlite3.Connection, settings: Settings, obs_date: str, until_ms: int | None = None) -> RunSummary:
    """Recompute one observation date from scratch under its own run label."""
    run = f"replay:{obs_date}"
    for table in ("paper_fills", "paper_signals", "paper_measurements"):
        conn.execute(f"DELETE FROM {table} WHERE run=?", (run,))
    window = _date_window(conn, obs_date)
    if window is None:
        return RunSummary(run=run, notes=["no markets for this date"])
    until = min(window[1], until_ms) if until_ms else window[1]
    s = process(conn, settings, run, window[0] - 1, until, only_date=obs_date)
    return s


def _totals(conn: sqlite3.Connection, run: str, obs_date: str, until_ms: int) -> dict[str, tuple[int, float]]:
    rows = conn.execute(
        """SELECT f.ledger, COUNT(*), COALESCE(SUM(f.notional + f.fee), 0) FROM paper_fills f JOIN paper_signals g ON g.id = f.signal_id
           WHERE f.run=? AND f.obs_date=? AND f.variant='conservative' AND g.ts_ms<=? GROUP BY f.ledger""",
        (run, obs_date, until_ms),
    ).fetchall()
    return {r[0]: (r[1], float(r[2])) for r in rows}


def reconcile(conn: sqlite3.Connection, settings: Settings, obs_date: str, now: int | None = None) -> dict:
    """Replay ``obs_date`` and compare with the ledger of record; pause everything if they differ."""
    now = now or now_ms()
    cursor = _cursor(conn) or 0
    replay_date(conn, settings, obs_date, until_ms=cursor)
    live, again = _totals(conn, LIVE, obs_date, cursor), _totals(conn, f"replay:{obs_date}", obs_date, cursor)
    out = {"obs_date": obs_date, "ledgers": {}, "ok": True}
    for name in sorted(set(live) | set(again) | set(LEDGERS)):
        a, b = live.get(name, (0, 0.0)), again.get(name, (0, 0.0))
        diff = abs(a[1] - b[1])
        out["ledgers"][name] = {"live_fills": a[0], "replay_fills": b[0], "live_usd": round(a[1], 4), "replay_usd": round(b[1], 4),
                                "difference_usd": round(diff, 4)}
        if diff > RECONCILE_TOLERANCE_USD:
            out["ok"] = False
    conn.execute(
        "INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (f"paper_reconciled:{obs_date}", json.dumps(out)),
    )
    if not out["ok"]:
        conn.execute("INSERT INTO pause_log(ts_ms, scope, action, trigger, detail) VALUES(?, 'all', 'pause', 'reconciliation', ?)",
                     (now, json.dumps(out)))
        log.error("paper ledgers paused: replay of %s differs from the ledger: %s", obs_date, out)
    return out


def reconcile_due(conn: sqlite3.Connection, settings: Settings, now: int) -> list[str]:
    """Reconcile every observation date that is over (all its markets' local days ended an hour ago) and not yet done."""
    notes = []
    cursor = _cursor(conn) or 0
    dates = [r[0] for r in conn.execute("SELECT DISTINCT obs_date FROM paper_signals WHERE run=? ORDER BY obs_date", (LIVE,))]
    for obs_date in dates:
        if conn.execute("SELECT 1 FROM kv WHERE key=?", (f"paper_reconciled:{obs_date}",)).fetchone():
            continue
        window = _date_window(conn, obs_date)
        day = dt.date.fromisoformat(obs_date)
        last_local_day_end = int(dt.datetime.combine(day + dt.timedelta(days=1), dt.time(0), UTC).timestamp() * 1000) + 12 * 3600_000
        if window is None or cursor < last_local_day_end:
            continue
        r = reconcile(conn, settings, obs_date, now)
        notes.append(f"reconciled {obs_date}: {'ok' if r['ok'] else 'DIFFERENT, ledgers paused'}")
    return notes


# ---- drawdown ----------------------------------------------------------------------------------


def realized(conn: sqlite3.Connection, run: str, name: str, variant: str = "conservative") -> float:
    """Profit or loss of settled positions."""
    return float(conn.execute(
        "SELECT COALESCE(SUM(payout - notional - fee), 0) FROM paper_fills WHERE run=? AND ledger=? AND variant=? AND payout IS NOT NULL",
        (run, name, variant)).fetchone()[0])


def check_drawdown(conn: sqlite3.Connection, now: int) -> list[str]:
    notes = []
    for name, rules in LEDGERS.items():
        if realized(conn, LIVE, name) <= -DRAWDOWN_SHARE * rules.bankroll_usd and not ledger.paused_reason(conn, now, "-", name):
            conn.execute("INSERT INTO pause_log(ts_ms, scope, action, trigger, detail) VALUES(?, ?, 'pause', 'drawdown', ?)",
                         (now, f"ledger:{name}", f"down {DRAWDOWN_SHARE:.0%} of the starting {rules.bankroll_usd:.0f} USD"))
            notes.append(f"ledger {name} paused: drawdown")
    return notes
