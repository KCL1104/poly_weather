"""Success criterion 1: does the rebuilt settlement value match the real outcome?

For every closed event with an 'ok' spec, the qualifying value is recomputed from the
archived METAR reports and compared with the bracket that actually resolved YES. A
mismatch on a live check freezes the station until someone has explained it.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
from dataclasses import dataclass

from ..catalog.scanner import freeze_station, load_brackets
from ..resolution.settle import settle
from ..resolution.spec import ResolutionSpec
from ..util.timeutil import now_ms
from ..weather.metar_store import load_obs

log = logging.getLogger(__name__)

# A day with fewer counted reports than this is not judged: the archive (not the rule)
# is the likelier cause of any difference, for example on the day the system is installed.
MIN_REPORTS = 20
AWC_SOURCES = ("awc_api", "awc_cache")
EDGE = dt.timedelta(minutes=90)  # an archive "spans" a day if it has reports this close to both ends


@dataclass
class CheckSummary:
    checked: int = 0
    matched: int = 0
    mismatched: int = 0
    no_data: int = 0
    no_winner: int = 0

    @property
    def rate(self) -> float | None:
        n = self.matched + self.mismatched
        return self.matched / n if n else None


def check_event(conn: sqlite3.Connection, event_id: str, *, sources: tuple[str, ...] | None = None) -> dict | None:
    row = conn.execute("SELECT spec_json FROM resolution_specs WHERE event_id=? AND status='ok'", (event_id,)).fetchone()
    if row is None:
        return None
    spec = ResolutionSpec.from_json(row["spec_json"])
    markets = load_brackets(conn, event_id)
    winners = [i for i, (m, _) in enumerate(markets) if m["winner"] == "YES"]
    actual = markets[winners[0]][1].label if len(winners) == 1 else None

    day_start, day_end = spec.local_day_bounds_utc()
    if sources is not None:
        s = settle(spec, load_obs(conn, spec.station, day_start, day_end, sources=sources))
        used = "+".join(sources)
    else:
        # Judge by the resolution source's own reports when its archive spans the whole
        # local day. Other archives fill in only for days AWC no longer serves; mixing
        # them in otherwise could add reports AWC never showed.
        awc_obs = load_obs(conn, spec.station, day_start - EDGE, day_end + EDGE, sources=AWC_SOURCES)
        spans = any(o.obs_time <= day_start + EDGE for o in awc_obs) and any(o.obs_time >= day_end - EDGE for o in awc_obs)
        if spans:
            s, used = settle(spec, awc_obs), "awc"
        else:
            s, used = settle(spec, load_obs(conn, spec.station, day_start, day_end)), "all"
    rebuilt = None
    if s.value_unit is not None and s.n_counted >= MIN_REPORTS:
        idx = next((i for i, (_, b) in enumerate(markets) if b.contains(s.value_unit)), None)
        rebuilt = markets[idx][1].label if idx is not None else None

    is_match = None if (actual is None or rebuilt is None) else int(actual == rebuilt)
    out = {
        "event_id": event_id,
        "station": spec.station,
        "obs_date": spec.obs_date,
        "measure": spec.measure,
        "n_counted": s.n_counted,
        "value_tenths_c": s.value_tenths_c,
        "value_unit": s.value_unit,
        "reconstructed_bracket": rebuilt,
        "actual_bracket": actual,
        "is_match": is_match,
        "detail": json.dumps(
            {"n_reports": s.n_reports, "deciding": list(s.deciding)[:3], "too_few_reports": s.n_counted < MIN_REPORTS,
             "sources": used}
        ),
    }
    conn.execute(
        """INSERT INTO settlement_checks(event_id, station, obs_date, measure, n_counted, value_tenths_c,
               value_unit, reconstructed_bracket, actual_bracket, is_match, detail, checked_ms)
           VALUES(:event_id, :station, :obs_date, :measure, :n_counted, :value_tenths_c, :value_unit,
                  :reconstructed_bracket, :actual_bracket, :is_match, :detail, :checked_ms)
           ON CONFLICT(event_id) DO UPDATE SET
               n_counted=excluded.n_counted, value_tenths_c=excluded.value_tenths_c,
               value_unit=excluded.value_unit, reconstructed_bracket=excluded.reconstructed_bracket,
               actual_bracket=excluded.actual_bracket, is_match=excluded.is_match, detail=excluded.detail,
               checked_ms=excluded.checked_ms""",
        {**out, "checked_ms": now_ms()},
    )
    return out


def run(
    conn: sqlite3.Connection,
    *,
    since: str | None = None,
    until: str | None = None,
    stations: list[str] | None = None,
    freeze_on_mismatch: bool = False,
    sources: tuple[str, ...] | None = None,
) -> CheckSummary:
    """Check closed events whose observation date lies in [since, until]."""
    q = """SELECT e.event_id, e.slug FROM events e JOIN resolution_specs s ON s.event_id = e.event_id
           WHERE e.closed = 1 AND s.status = 'ok'"""
    args: list = []
    if since:
        q += " AND s.obs_date >= ?"
        args.append(since)
    if until:
        q += " AND s.obs_date <= ?"
        args.append(until)
    if stations:
        q += f" AND s.station IN ({','.join('?' * len(stations))})"
        args += stations
    summary = CheckSummary()
    for row in conn.execute(q + " ORDER BY s.obs_date, s.station", args).fetchall():
        before = conn.execute("SELECT is_match FROM settlement_checks WHERE event_id=?", (row["event_id"],)).fetchone()
        already_known = before is not None and before["is_match"] == 0
        r = check_event(conn, row["event_id"], sources=sources)
        if r is None:
            continue
        summary.checked += 1
        if r["actual_bracket"] is None:
            summary.no_winner += 1
        elif r["reconstructed_bracket"] is None:
            summary.no_data += 1
        elif r["is_match"]:
            summary.matched += 1
        else:
            summary.mismatched += 1
            log.warning(
                "settlement mismatch %s: rebuilt %s, actual %s", row["slug"],
                r["reconstructed_bracket"], r["actual_bracket"],
            )
            # A mismatch freezes its station once. Re-checking the same event after a
            # human has explained it and thawed the station must not freeze it again.
            if freeze_on_mismatch and not already_known:
                freeze_station(conn, r["station"], "settlement_mismatch", row["slug"])
    return summary
