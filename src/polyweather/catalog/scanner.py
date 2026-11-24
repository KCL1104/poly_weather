"""Market catalog: scan Gamma, store events and markets, parse rules, detect changes.

A change to an existing event's rule text or resolution source, or a new event whose
parsed rules differ from the station's previous ones, is written to ``change_log`` and
freezes the station (``station_status``). Thawing is a deliberate human step.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from typing import Iterable

from ..polymarket import gamma
from ..resolution.spec import PARSER_VERSION, ResolutionSpec, parse_spec, rules_hash
from ..util.timeutil import now_ms
from .brackets import Bracket, parse_bracket, validate_ladder

log = logging.getLogger(__name__)


@dataclass
class ScanResult:
    events_seen: int = 0
    events_new: int = 0
    markets_seen: int = 0
    unparsed: int = 0
    changes: list[dict] = field(default_factory=list)
    frozen: list[str] = field(default_factory=list)


def freeze_station(conn: sqlite3.Connection, station: str, trigger: str, detail: str, ts: int | None = None) -> bool:
    """Mark a station frozen. Returns True if it was not frozen before."""
    ts = ts or now_ms()
    row = conn.execute("SELECT state FROM station_status WHERE station=?", (station,)).fetchone()
    if row and row["state"] == "frozen":
        return False
    conn.execute(
        "INSERT INTO station_status(station, state, reason, since_ms) VALUES(?, 'frozen', ?, ?) "
        "ON CONFLICT(station) DO UPDATE SET state='frozen', reason=excluded.reason, since_ms=excluded.since_ms",
        (station, f"{trigger}: {detail}", ts),
    )
    conn.execute(
        "INSERT INTO pause_log(ts_ms, scope, action, trigger, detail) VALUES(?, ?, 'pause', ?, ?)",
        (ts, f"station:{station}", trigger, detail),
    )
    return True


def thaw_station(conn: sqlite3.Connection, station: str, note: str, ts: int | None = None) -> None:
    ts = ts or now_ms()
    conn.execute(
        "INSERT INTO station_status(station, state, reason, since_ms) VALUES(?, 'active', ?, ?) "
        "ON CONFLICT(station) DO UPDATE SET state='active', reason=excluded.reason, since_ms=excluded.since_ms",
        (station, note, ts),
    )
    conn.execute(
        "INSERT INTO pause_log(ts_ms, scope, action, trigger, detail) VALUES(?, ?, 'resume', 'manual', ?)",
        (ts, f"station:{station}", note),
    )


def is_frozen(conn: sqlite3.Connection, station: str) -> bool:
    row = conn.execute("SELECT state FROM station_status WHERE station=?", (station,)).fetchone()
    return bool(row and row["state"] == "frozen")


def _ordered_markets(event: dict) -> list[dict]:
    def key(m: dict):
        try:
            return (0, float(m.get("groupItemThreshold")))
        except (TypeError, ValueError):
            return (1, 0.0)

    return sorted(event.get("markets") or [], key=key)


def _log_change(conn, ts, event_id, station, fld, old, new, result: ScanResult) -> None:
    conn.execute(
        "INSERT INTO change_log(ts_ms, event_id, station, field, old_value, new_value) VALUES(?,?,?,?,?,?)",
        (ts, event_id, station, fld, old, new),
    )
    result.changes.append({"event_id": event_id, "station": station, "field": fld, "old": old, "new": new})


def upsert_event(
    conn: sqlite3.Connection,
    event: dict,
    result: ScanResult,
    ts: int | None = None,
    *,
    detect_changes: bool = True,
) -> ResolutionSpec:
    """Store one Gamma event and its markets; returns the parsed spec.

    ``detect_changes=False`` is for importing history, where rule changes that already
    happened must not freeze anything today.
    """
    ts = ts or now_ms()
    event_id = str(event["id"])
    rules_text = event.get("description") or ""
    r_hash = rules_hash(rules_text)
    source = event.get("resolutionSource") or ""
    markets = _ordered_markets(event)
    brackets: list[Bracket | None] = [parse_bracket(m.get("groupItemTitle") or "") for m in markets]
    ladder_problems = validate_ladder(brackets)

    spec = parse_spec(
        rules_text=rules_text,
        resolution_source=source,
        event_date=event.get("eventDate"),
        title=event.get("title"),
        bracket_units={b.unit for b in brackets if b is not None} or None,
    )
    if ladder_problems:
        spec = ResolutionSpec(
            **{**spec.__dict__, "status": "unparsed", "reasons": tuple(spec.reasons) + tuple(ladder_problems)}
        )
    # A market whose own rule text or source differs from its event's is not safe to group.
    for m in markets:
        if (m.get("description") or "") != rules_text or (m.get("resolutionSource") or "") != source:
            spec = ResolutionSpec(
                **{**spec.__dict__, "status": "unparsed", "reasons": tuple(spec.reasons) + ("market_rules_differ_from_event",)}
            )
            break

    prev = conn.execute(
        "SELECT rules_hash, resolution_source FROM events WHERE event_id=?", (event_id,)
    ).fetchone()
    # An event that stops parsing no longer names a station; the freeze must still land
    # on the station it was resolved by until now.
    before_row = conn.execute("SELECT station FROM resolution_specs WHERE event_id=?", (event_id,)).fetchone()
    station = spec.station or (before_row["station"] if before_row else None)
    result.events_seen += 1
    if prev is None:
        result.events_new += 1
    elif detect_changes:
        changed = []
        if prev["rules_hash"] != r_hash:
            changed.append(("rules_hash", prev["rules_hash"], r_hash))
        if (prev["resolution_source"] or "") != source:
            changed.append(("resolution_source", prev["resolution_source"], source))
        for fld, old, new in changed:
            _log_change(conn, ts, event_id, station, fld, old, new, result)
            if station and freeze_station(conn, station, "rules_changed", f"{event['slug']}: {fld}", ts):
                result.frozen.append(station)

    conn.execute(
        """INSERT INTO events(event_id, slug, title, series_slug, event_date, start_date, end_date, closed,
               closed_time, resolution_source, rules_text, rules_hash, neg_risk, volume, first_seen_ms, last_seen_ms)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(event_id) DO UPDATE SET
               title=excluded.title, series_slug=excluded.series_slug, event_date=excluded.event_date,
               start_date=excluded.start_date, end_date=excluded.end_date, closed=excluded.closed,
               closed_time=excluded.closed_time, resolution_source=excluded.resolution_source,
               rules_text=excluded.rules_text, rules_hash=excluded.rules_hash, neg_risk=excluded.neg_risk,
               volume=excluded.volume, last_seen_ms=excluded.last_seen_ms""",
        (
            event_id, event["slug"], event.get("title"), event.get("seriesSlug"), event.get("eventDate"),
            event.get("startDate"), event.get("endDate"), int(bool(event.get("closed"))), event.get("closedTime"),
            source, rules_text, r_hash, int(bool(event.get("negRisk"))), event.get("volume"), ts, ts,
        ),
    )

    for idx, (m, b) in enumerate(zip(markets, brackets)):
        tokens = gamma.token_ids(m)
        fee = m.get("feeSchedule") or {}
        result.markets_seen += 1
        conn.execute(
            """INSERT INTO markets(market_id, event_id, slug, question, group_title, group_index, condition_id,
                   yes_token, no_token, lo, hi, unit, bracket_ok, fee_rate, fee_exponent, fee_taker_only,
                   fee_schedule, tick_size, min_order_size, neg_risk, closed, closed_time, winner,
                   outcome_prices, volume, first_seen_ms, last_seen_ms)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(market_id) DO UPDATE SET
                   question=excluded.question, group_title=excluded.group_title, group_index=excluded.group_index,
                   yes_token=excluded.yes_token, no_token=excluded.no_token, lo=excluded.lo, hi=excluded.hi,
                   unit=excluded.unit, bracket_ok=excluded.bracket_ok, fee_rate=excluded.fee_rate,
                   fee_exponent=excluded.fee_exponent, fee_taker_only=excluded.fee_taker_only,
                   fee_schedule=excluded.fee_schedule, tick_size=excluded.tick_size,
                   min_order_size=excluded.min_order_size, neg_risk=excluded.neg_risk, closed=excluded.closed,
                   closed_time=excluded.closed_time, winner=excluded.winner,
                   outcome_prices=excluded.outcome_prices, volume=excluded.volume,
                   last_seen_ms=excluded.last_seen_ms""",
            (
                str(m["id"]), event_id, m.get("slug"), m.get("question"), m.get("groupItemTitle"), idx,
                m.get("conditionId"), tokens[0] if tokens else None, tokens[1] if tokens else None,
                b.lo if b else None, b.hi if b else None, b.unit if b else None,
                int(b is not None and not ladder_problems),
                fee.get("rate"), fee.get("exponent"),
                None if fee.get("takerOnly") is None else int(bool(fee.get("takerOnly"))),
                json.dumps(fee, sort_keys=True) if fee else None,
                m.get("orderPriceMinTickSize"), m.get("orderMinSize"), int(bool(m.get("negRisk"))),
                int(bool(m.get("closed"))), m.get("closedTime"), gamma.winner(m),
                m.get("outcomePrices") if isinstance(m.get("outcomePrices"), str) else json.dumps(m.get("outcomePrices")),
                m.get("volumeNum"), ts, ts,
            ),
        )

    # A new event whose rules parse differently from the previous event of the same
    # series is a rule change even though no single event was edited.
    if detect_changes and prev is None and event.get("seriesSlug"):
        last = conn.execute(
            """SELECT s.spec_json FROM resolution_specs s JOIN events e ON e.event_id = s.event_id
               WHERE e.series_slug=? AND e.event_date < ? AND s.event_id != ?
               ORDER BY e.event_date DESC LIMIT 1""",
            (event["seriesSlug"], event.get("eventDate") or "", event_id),
        ).fetchone()
        if last:
            before = ResolutionSpec.from_json(last["spec_json"])
            if before.ok and before.signature() != spec.signature():
                affected = {x for x in (before.station, station) if x}
                for st in sorted(affected):
                    _log_change(conn, ts, event_id, st, "spec_signature", before.signature(), spec.signature(), result)
                    if freeze_station(conn, st, "rules_changed", f"{event['slug']}: new event parses differently", ts):
                        result.frozen.append(st)

    conn.execute(
        """INSERT INTO resolution_specs(event_id, status, reasons, template_id, station, measure, unit, obs_date,
               tz, source_url, window_start_utc, window_end_utc, spec_json, parser_version, parsed_ms)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(event_id) DO UPDATE SET
               status=excluded.status, reasons=excluded.reasons, template_id=excluded.template_id,
               station=excluded.station, measure=excluded.measure, unit=excluded.unit, obs_date=excluded.obs_date,
               tz=excluded.tz, source_url=excluded.source_url, window_start_utc=excluded.window_start_utc,
               window_end_utc=excluded.window_end_utc, spec_json=excluded.spec_json,
               parser_version=excluded.parser_version, parsed_ms=excluded.parsed_ms""",
        (
            event_id, spec.status, json.dumps(list(spec.reasons)), spec.template_id, spec.station, spec.measure,
            spec.unit, spec.obs_date, spec.tz, spec.source_url, spec.window_start_utc, spec.window_end_utc,
            spec.to_json(), PARSER_VERSION, ts,
        ),
    )
    if not spec.ok:
        result.unparsed += 1
    return spec


def ingest(conn: sqlite3.Connection, events: Iterable[dict], *, detect_changes: bool = True) -> ScanResult:
    result = ScanResult()
    conn.execute("BEGIN")
    try:
        for e in events:
            upsert_event(conn, e, result, detect_changes=detect_changes)
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return result


def scan(
    conn: sqlite3.Connection, client, *, closed_since: str | None = None, detect_changes: bool = True
) -> ScanResult:
    """One catalog pass: every open weather event, plus events closed since ``closed_since``."""
    result = ingest(conn, gamma.weather_events(client, closed=False), detect_changes=detect_changes)
    if closed_since:
        r2 = ingest(
            conn,
            gamma.weather_events(client, closed=True, end_date_min=closed_since),
            detect_changes=detect_changes,
        )
        result.events_seen += r2.events_seen
        result.events_new += r2.events_new
        result.markets_seen += r2.markets_seen
        result.unparsed += r2.unparsed
        result.changes += r2.changes
        result.frozen += r2.frozen
    log.info(
        "catalog scan: %d events (%d new), %d markets, %d unparsed, %d changes, frozen=%s",
        result.events_seen, result.events_new, result.markets_seen, result.unparsed,
        len(result.changes), sorted(set(result.frozen)),
    )
    return result


def load_brackets(conn: sqlite3.Connection, event_id: str) -> list[tuple[sqlite3.Row, Bracket]]:
    rows = conn.execute(
        "SELECT * FROM markets WHERE event_id=? ORDER BY group_index", (event_id,)
    ).fetchall()
    return [(r, Bracket(r["lo"], r["hi"], r["unit"])) for r in rows]
